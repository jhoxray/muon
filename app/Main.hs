{-# LANGUAGE OverloadedStrings, TransformListComp, FlexibleContexts #-}

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

import Control.DeepSeq

import Control.Monad.IO.Class -- liftIO !!!

-- import qualified Data.Map as Map
-- import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as Map
import Data.Hashable

import qualified Data.Vector as V
import qualified Data.Vector.Generic as G

import Quark.Base.Data
import Quark.Base.Column
import Quark.Base.Aggregation
import Quark.Base.Storage

import SSCSV


helpMsg = "Welcome to HasGraph terminal! For the list of available commands, try :commands"
unknownMsg = "Unknown input! \n\
    \try :commands or :help"


-- this is ugly quick hack to test stuff
data GlobalState = GlobalState {
                      ctb :: CTable
                   }

emptyGS = GlobalState {ctb = Map.fromList []}

commandsList = 
    [ (":ptime",    ("show current ptime", (\x -> liftIO getCPUTime >>= outputStrLn . show )) )
    , (":time",     ("show current time",  (\x -> liftIO getCurrentTime >>= outputStrLn . show)) )
    , (":env",      ("show Environment", cmdEnv) )
    , (":commands", ("show all available commands", cmdCommandsHelp) )
    , (":runColumn", ("run aggregation via column based storage in-memory (run :load first!!!)", cmdColumnRunMem) )
    ]

commands = Map.fromList commandsList
prompt = "hs>> "


cmdLoadCTable :: GlobalState -> IO GlobalState
cmdLoadCTable gs = do
    t3 <- getCurrentTime
    ct <- loadCTable "/Users/aantich/temp/db2"
    let (Just reg1, Just subreg1, Just terr1, Just am1) = (Map.lookup "globalRegion" ct, Map.lookup "region" ct, Map.lookup "subregion" ct, Map.lookup "amount" ct)
    let (reg, subreg, terr, am) = (unpackCText reg1, unpackCText subreg1, unpackCText terr1, unpackCDouble am1)
    print $ show (G.length reg) -- forcing lazy load to execute - there's a better way to do it I'm sure
    t4 <- getCurrentTime
    print $ "Time elapsed: " ++ show (diffUTCTime t4 t3)
    return GlobalState {ctb = ct}


cmdColumnRunMem :: GlobalState -> InputT IO ()    
cmdColumnRunMem gs = do
      let ct = ctb gs
      let (Just reg1, Just subreg1, Just terr1, Just am1) = (Map.lookup "globalRegion" ct, Map.lookup "region" ct, Map.lookup "subregion" ct, Map.lookup "amount" ct)
      let (reg, subreg, terr, am) = (unpackCText reg1, unpackCText subreg1, unpackCText terr1, unpackCDouble am1)
      -- outputStrLn $ show (G.length reg, G.length am, G.length subreg, G.length terr)

      t3 <- liftIO getCurrentTime
      let output1 = groupColumns3 (reg, subreg, terr) (+) am
      t4 <- liftIO $ output1 `deepseq` getCurrentTime
      outputStrLn $ show output1
      outputStrLn $ "Time elapsed in Column 2nd time: " ++ show (diffUTCTime t4 t3)
      

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
loop :: GlobalState -> InputT IO ()
loop gs = do
   minput <- getInputLine prompt
   case minput of
       Nothing -> return ()
       Just ":quit"     -> return ()
       Just ":help"     -> outputStrLn helpMsg >> loop gs
       Just ":load"     -> liftIO (cmdLoadCTable gs) >>= loop
                               
       Just input       -> let c = Map.lookup input commands
                           in case c of 
                                (Just cmd) -> do t1 <- liftIO getCurrentTime
                                                 (snd cmd) gs
                                                 t2 <- liftIO getCurrentTime
                                                 -- liftIO $ fprint (timeSpecs % "\n") t1 t2
                                                 outputStrLn $ "Time elapsed: " ++ show (diffUTCTime t2 t1)
                                                 -- outputStrLn $ "Time elapsed (ps) " ++ show (t2 - t1)
                                                 loop gs
                                Nothing -> outputStrLn unknownMsg >> loop gs

                        
main :: IO ()
main = runInputT defaultSettings (loop emptyGS)