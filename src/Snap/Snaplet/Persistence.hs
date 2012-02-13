{-# OPTIONS -XOverloadedStrings 
            -XTypeFamilies 
            -XFlexibleContexts 
            -XScopedTypeVariables 
            -XTemplateHaskell 
            -XDeriveDataTypeable 
            -XTupleSections 
#-}
module Snap.Snaplet.Persistence 
       ( Persistence ()
       , HasPersistence (..)
       , AesonPersistence (..)
       , initPersistence
       , persist
       , persistSTM
       , restore
       )
       where

import           Data.Monoid
import           Data.Maybe
import qualified Data.List as L
import           Data.Typeable
import           Data.Time.Clock.POSIX
import           Blaze.Text.Int
import           Blaze.ByteString.Builder
import           Data.Lens.Common
import           Data.Lens.Template
import           Data.Unique
import qualified Data.HashMap.Lazy       as HM
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy.Internal as TL
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BSL
import qualified Data.Aeson              as Aeson
import qualified Data.Aeson.Types        as Aeson
import qualified Data.Aeson.Encode       as Aeson
import qualified Data.Aeson.Parser       as Aeson

import           Control.Exception (Exception)

import Snap

import Control.Concurrent.STM
import Control.Concurrent (forkIO, ThreadId)

import System.IO
import System.Directory as Dir
import System.FilePath

--imports for the restore capability

import Data.Iteratee
import Data.Iteratee.IO.Handle
import Data.Iteratee.Char


import Data.Attoparsec       as Atto
import Data.Attoparsec.Char8 as AttoC

atomIO :: (MonadIO m) => STM a -> m a
atomIO  = liftIO . atomically

data PersistenceState b
  = Offline
  | Locked
  | Ready   (WorkerInterface b)

data WorkerInterface b
  = WorkerInterface
    { _writeChannel    :: Event b -> STM ()
    , _getEventCounter :: STM Int
    , _getLogfilePath  :: STM FilePath
    , _worker          :: Unique
    }

data Worker b
  = Worker
    { _initialEvents     :: [Event b]
    , _eventChannel      :: TChan (Event b)
    , _eventCounter      :: TVar Int
    , _logfileHandle     :: Handle
    , _identifier        :: Unique
    , _getCurrentWorker  :: STM (Maybe Unique)
    }

data Persistence b
  = Persistence
    { _status             :: TVar (PersistenceState b)
    }

class (AesonPersistence (Event b)) => HasPersistence b where
  type Event b
  persistenceLens :: Lens (Snaplet b) (Snaplet (Persistence b))
  resetState      :: Handler a b ()
  dumpState       :: Handler a b (STM [Event b])
  replayEvent     :: Event b -> Handler a b ()

class AesonPersistence a where
  toAeson         :: a -> Aeson.Value
  parseAeson      :: Aeson.Value -> Aeson.Parser a

$(makeLenses [''WorkerInterface, ''Worker, ''Persistence])

initPersistence :: HasPersistence b => Lens (Snaplet a) (Snaplet b) -> SnapletInit a (Persistence b)
initPersistence l
  = makeSnaplet
      "persistence"
      ""
      Nothing
      $ do -- create the directory to read and write from/to
           -- figure out most recent logfile
           p  <- getSnapletFilePath
           liftIO $ Dir.createDirectoryIfMissing True p
           addRoutes
             [ ("status",  handleStatus  l)
             , ("dump",    handleDump    l)
             , ("reset",   handleReset   l)
             , ("restore", handleRestore l)
             ]
           Persistence <$> liftIO (newTVarIO Offline)


-- | Returns a handle in write mode.
newLogfile :: Handler a (Persistence b) (FilePath, Handle)
newLogfile
  = do lfs <- listLogfiles
       p   <- getSnapletFilePath
       f   <- case lfs of
                [] -> return "0"
                xs -> return $ show $ L.last xs + 1
       let fp = p </> f <.> "log"
       (fp,) <$> (liftIO $ openFile fp WriteMode)

-- | Open Logfile for reading.
openLogfile :: Int -> Handler a (Persistence b) (Maybe Handle)
openLogfile i
  = do p <- getSnapletFilePath
       liftIO $ Just <$> openFile (p </> show i <.> "log") ReadMode

-- | Maybe returns a handle in readmode.
latestLogfile :: Handler a (Persistence b) (Maybe Handle)
latestLogfile
  = do p   <- getSnapletFilePath
       lfs <- listLogfiles
       case lfs of
         [] -> return Nothing
         xs -> liftIO $ Just <$> openFile (p </> show (L.last xs) <.> "log") ReadMode

listLogfiles :: Handler a (Persistence b) [Int]
listLogfiles
  = do p <- getSnapletFilePath
       cs <- liftIO $ Dir.getDirectoryContents p
       return $ L.sort $ mapMaybe f cs
  where 
    f x    
      = let     ds = L.takeWhile isDigit x
        in let  rs = L.dropWhile isDigit x
        in if   not (null ds) && rs == ".log"
           then Just (read ds)
           else Nothing


persist :: (HasPersistence b) => Event b -> Handler a b ()
persist e
  = do f <- persistSTM
       liftIO $ atomically $ f e

persistSTM :: (HasPersistence b) => Handler a b (Event b -> STM ())
persistSTM
  = do p <- with' persistenceLens $ get
       return $ \x-> do q <- readTVar (p ^. status)
                        case q of
                          Offline -> throwSTM InstanceOffline
                          Locked  -> throwSTM InstanceLocked
                          Ready s -> (s ^. writeChannel) x

data PersistenceException 
  = InstanceOffline
  | InstanceLocked
    deriving (Typeable, Show)

instance Exception PersistenceException

-- restore
----------

type EvNumber = Int
type Time     = Int
data RawEvent
  = Json        EvNumber Time Aeson.Value
  | Invalid     EvNumber Time BS.ByteString
  | Comment     T.Text
  | Garbage     BS.ByteString
  deriving (Eq, Show)

-- | Dumps the persistence status to a new logfile. Further events will be appended to that log.
dumpSTM :: forall a b. (HasPersistence b) => Handler a b (STM (Handler a b ()))
dumpSTM
  = do dumpStateSTM   <- dumpState -- just get the function, execution within transaction
       p              <- with' persistenceLens get
       unique         <- liftIO $ newUnique

       -- start STM transaction
       return $ do counter <- newTVar 0
                   channel <- newTChan
                   events  <- dumpStateSTM                            
                   path    <- newEmptyTMVar -- empty for as long as we haven't aquired a file handle yet
                   let q = WorkerInterface
                           { _writeChannel     = writeTChan channel 
                           , _getEventCounter  = readTVar counter
                           , _getLogfilePath   = readTMVar path
                           , _worker           = unique 
                           }
                   let w handle = 
                           Worker
                           { _initialEvents    = events
                           , _eventChannel     = channel
                           , _eventCounter     = counter
                           , _logfileHandle    = handle 
                           , _identifier       = unique
                           , _getCurrentWorker = do u <- readTVar (p ^. status)
                                                    case u of
                                                      Ready x -> return $ Just (x ^. worker)
                                                      _       -> return Nothing
                           }
                   writeTVar (p ^. status) (Ready q)
                   return $ do (fp, handle) <- with' persistenceLens newLogfile
                               atomIO $ putTMVar path fp
                               liftIO $ forkIO $ process $ (w handle :: Worker b)
                               return ()
       -- end STM transaction
  where
    process :: (HasPersistence b) => Worker b -> IO ()
    process this
      = case this ^. initialEvents of
          []     -> do me <- atomically $ do empty         <- isEmptyTChan (this ^. eventChannel)
                                             currentWorker <- this ^. getCurrentWorker
                                             if empty && (currentWorker /= Just (this ^. identifier))
                                               then return Nothing -- queue is empty and worker is outdated
                                               else do i <- readTVar (this ^. eventCounter)
                                                       writeTVar (this ^. eventCounter) (i+1)
                                                       e <- readTChan (this ^. eventChannel)
                                                       return $ Just (e,i)
                       case me of
                         Nothing    -> hClose (this ^. logfileHandle) -- flush and close handle and terminate thread
                         Just (e,i) -> writeEvent e i >> process this -- recurse and continue processing
          -- initial events are written even if the thread is to be terminated
          (x:xs) -> do i <- atomically $ do i <- readTVar (this ^. eventCounter)
                                            writeTVar (this ^. eventCounter) $! (i + 1)
                                            return i
                       writeEvent x i
                       process (setL initialEvents xs this)
      where
        writeEvent e i
          = do i <- (toLazyByteString . integral) <$> return i
               t <- (toLazyByteString . integral) <$> truncate <$> (*1000) <$> getPOSIXTime
               BSL.hPut (this ^. logfileHandle) 
                 $ mconcat [ BSL.replicate (15 - fromIntegral (BSL.length i)) 32
                           , i
                           , ":"
                           , BSL.replicate (15 - fromIntegral (BSL.length t)) 32
                           , t
                           , ": "
                           , Aeson.encode (toAeson e)
                           , "\n"
                           ]
               hFlush (this ^. logfileHandle)

dump  :: forall a b. (HasPersistence b) => Handler a b ()
dump
  = do p <- with' persistenceLens get
       dumpSTM' <- dumpSTM
       join $ atomIO $ do st <- readTVar (p ^. status)
                          case st of
                            Locked -> throwSTM InstanceLocked
                            _      -> dumpSTM'

reset :: forall a b. (HasPersistence b) => Handler a b ()
reset
  = do 
       p <- with' persistenceLens get 
       atomIO $ do st <- readTVar (p ^. status)
                   case st of
                     Locked -> throwSTM InstanceLocked
                     _      -> writeTVar (p ^.status) Locked
       resetState
       join . atomIO =<< dumpSTM

restore  :: forall a b. (HasPersistence b) => Handle -> Handler a b ()
restore h
  = do p <- with' persistenceLens get
       atomIO $ do st <- readTVar (p ^. status)
                   case st of
                     Locked -> throwSTM InstanceLocked 
                     _      -> writeTVar (p ^. status) Locked
       resetState
       run =<< enumHandle (1024*16) h eventProcessor
       liftIO $ hClose h
       join . atomIO =<< dumpSTM
  where
    eventProcessor :: Iteratee BS.ByteString (Handler a b) ()
    eventProcessor
      = joinI $ (enumLines ><> mapStream (Atto.parseOnly parseEvent)) (Data.Iteratee.mapM_ replay) 
    replay    :: Either String RawEvent -> Handler a b () 
    replay (Left s)
      = return ()
    replay (Right ev)
      = do setTimeout <- getTimeoutAction
           liftIO $ setTimeout 1
           case ev of
             (Json n t j)    -> case Aeson.parse parseAeson (j :: Aeson.Value) of
                                  (Aeson.Error e)   -> return ()
                                  (Aeson.Success v) -> replayEvent v
             (Invalid n t b) -> return ()
             (Comment t)     -> return ()
             (Garbage b)     -> return ()
    parseEvent     :: Parser RawEvent
    parseEvent
      = AttoC.choice
          [ do AttoC.skipSpace
               n <- AttoC.decimal
               AttoC.char ':'
               AttoC.skipSpace
               t <- AttoC.decimal
               AttoC.char ':'
               AttoC.skipSpace
               Atto.choice
                 [ Json    n t <$> Aeson.json'  
                 , Invalid n t <$> AttoC.takeByteString
                 ]
          , do AttoC.string "--"
               AttoC.skipSpace
               Comment <$> T.decodeUtf8 <$> AttoC.takeByteString
          , do Garbage <$> AttoC.takeByteString 
          ]

-- standalone handlers
----------------------

handleStatus  :: (HasPersistence b) => Lens (Snaplet a) (Snaplet b) -> Handler a (Persistence b) ()
handleStatus l
  = do st <- get >>= (atomIO . readTVar . _status)
       case st of
         Offline -> writeText "Offline."
         Locked  -> writeText "Locked."
         Ready x -> writeText "Ready."

handleDump    :: (HasPersistence b) => Lens (Snaplet a) (Snaplet b) -> Handler a (Persistence b) ()
handleDump l
  = do withTop' l $ dump
       writeText "Done."

handleReset    :: (HasPersistence b) => Lens (Snaplet a) (Snaplet b) -> Handler a (Persistence b) ()
handleReset l
  = do withTop' l $ reset
       writeText "Done."

handleRestore :: (HasPersistence b) => Lens (Snaplet a) (Snaplet b) -> Handler a (Persistence b) ()
handleRestore l
  = do mn <- getParam "number"
       mh <- case mn of
               Nothing -> latestLogfile
               Just n  -> let n' = T.unpack $ T.decodeUtf8 n
                          in if all isDigit n'
                               then openLogfile (read n')
                               else fail "'number' must only consist of digits"
       case mh of
         Nothing -> fail "Can't open logfile. Doesn't exist or permissions error."
         Just h  -> do withTop' l $ restore h
                       writeText "Done."
