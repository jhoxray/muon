module Main where

{- TODO: 
    - map based commands
    - proper basis data type (Money as integer etc) - so that it can be efficiently represented in Vectors eventually

-}

import System.IO
import System.Environment
import System.Console.Haskeline

import System.CPUTime
import Data.Time.Clock
import System.Info
import System.Environment

import Control.Monad.IO.Class -- liftIO !!!

import qualified Data.Map as Map

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
    ]

commands = Map.fromList commandsList
prompt = "hs>> "

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
loop :: InputT IO ()
loop = do
   minput <- getInputLine prompt
   case minput of
       Nothing -> return ()
       Just ":quit"     -> return ()
       Just ":help"     -> do outputStrLn helpMsg >> loop
       Just input       -> let c = Map.lookup input commands
                           in case c of 
                                (Just cmd) -> do t1 <- liftIO getCurrentTime
                                                 (snd cmd) 1 
                                                 t2 <- liftIO getCurrentTime
                                                 outputStrLn $ "Time elapsed (ps) " ++ show (diffUTCTime t2 t1)
                                                 loop
                                Nothing -> do outputStrLn unknownMsg >> loop

                        
main :: IO ()
main = runInputT defaultSettings loop