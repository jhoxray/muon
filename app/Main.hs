module Main where

{- TODO: 
    - map based commands
    - proper basis data type (Money as integer etc) - so that it can be efficiently represented in Vectors eventually

-}

import System.IO
import System.Environment
import System.Console.Haskeline

import Formatting
import Formatting.Clock
import System.Clock

import System.CPUTime
import Data.Time.Clock
import System.Info
import System.Environment

import Data.Csv

import Control.Monad.IO.Class -- liftIO !!!

-- import qualified Data.Map as Map
import qualified Data.HashMap as Map
import qualified Data.Vector as V

import Quark.Base.Data
import Quark.Base.Column

import SSCSV

helpMsg = "Welcome to HasGraph terminal! For the list of available commands, try :commands"
unknownMsg = "Unknown input! \n\
    \try :commands or :help"


commandsList = 
    [ (":ptime",    ("show current ptime", (\x -> liftIO getCPUTime >>= outputStrLn . show )) )
    , (":time",     ("show current time",  (\x -> liftIO getCurrentTime >>= outputStrLn . show)) )
    , (":env",      ("show Environment", cmdEnv) )
    , (":commands", ("show all available commands", cmdCommandsHelp) )
    , (":tr", ("test run with sample file", (\x -> liftIO (testRun "src/sample-l.csv") >>= outputStrLn . show)) )
    , (":runList", ("run aggregation via list generalized comprehensions", cmdListCompRun) )
    , (":runVecR", ("run aggregation via custom vector foldr'", cmdVectorR') )
    , (":runVecL", ("run aggregation via custom vector foldl'", cmdVectorL') )
    ]

commands = Map.fromList commandsList
prompt = "hs>> "

cmdLoadFile :: IO QDatabase
cmdLoadFile = do 
    (Right f) <- loadToMemory "src/sample-l.csv" HasHeader
    let pf = processFile encoders f
    return pf

cmdVectorR' :: QDatabase -> InputT IO ()
cmdVectorR' db = 
    let output = V.foldr' (\ x acc -> processAggrM2 14 5 9 acc x) (Map.fromList [] :: Map.Map [QValue] QValue) db
    in outputStrLn $ show output

cmdVectorL' :: QDatabase -> InputT IO ()
cmdVectorL' db = 
    let output = V.foldl' (\acc x -> processAggrM2 14 5 9 acc x) (Map.fromList [] :: Map.Map [QValue] QValue) db
    in outputStrLn $ show output


cmdListCompRun :: QDatabase -> InputT IO ()
cmdListCompRun db = 
    let pfl = convertFileToList db
        output = aggregateData pfl
    in outputStrLn $ show output

cmdColumnRun :: QDatabase -> InputT IO ()    
cmdColumnRun db = 
  let (reg, _, am) = convertDB' db
      output = vfold (+) (Map.fromList []) reg am
  in outputStrLn $ show output

cmdCommandsHelp :: t -> InputT IO ()
cmdCommandsHelp _ = do 
    mapM_ (outputStrLn . comb) commandsList
    where comb (s, (h, _) ) = s ++ " -- " ++ h

-- cmdTR :: t -> InputT IO ()
-- cmdTR 

cmdEnv :: t -> InputT IO ()
cmdEnv _ = do 
    env <- liftIO getEnvironment
    let tmp1 (k,v) = show k ++ " = " ++ show v
    mapM_ outputStrLn (map tmp1 env)

cmdSysinfo :: t -> InputT IO ()
cmdSysinfo _ = do 
    outputStrLn os
    outputStrLn arch
    outputStrLn compilerName
    outputStrLn $ show compilerVersion

-- main loop, processing commands
loop :: QDatabase -> InputT IO ()
loop db = do
   minput <- getInputLine prompt
   case minput of
       Nothing -> return ()
       Just ":quit"     -> return ()
       Just ":help"     -> outputStrLn helpMsg >> loop db
       Just ":load"     -> liftIO cmdLoadFile >>= loop
                               
       Just input       -> let c = Map.lookup input commands
                           in case c of 
                                (Just cmd) -> do t1 <- liftIO $ getSystemTime
                                                 (snd cmd) db
                                                 t2 <- liftIO $ getSystemTime
                                                 -- liftIO $ fprint (timeSpecs % "\n") t1 t2
                                                 outputStrLn $ "Time elapsed: " ++ show (diffUTCTime t2 t1)
                                                 -- outputStrLn $ "Time elapsed (ps) " ++ show (t2 - t1)
                                                 loop db
                                Nothing -> outputStrLn unknownMsg >> loop db

                        
main :: IO ()
main = runInputT defaultSettings (loop (V.fromList [V.fromList []]))