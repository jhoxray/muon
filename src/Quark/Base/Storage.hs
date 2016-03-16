{-# LANGUAGE OverloadedStrings, RankNTypes, TypeSynonymInstances, FlexibleInstances, OverloadedLists, DeriveGeneric  #-}

{-
    Column datastore approach.
    Starting with a lightweight directory structure - 1 database per 1 running instance, thus:
    SYSDIR
       |---- metadata:      file with database metadata
       |---- Table1Dir:     folder for a specific table
                |------ metadata:   file with table structure metadata
                |------ RAW:        folder with raw columns
                         |------ metadata:   file with raw columns metadata (names and types basically?)
                         |------ column0:    file with Column0 raw data
                         |------ column1:    file with Column1 raw data (automatically parsed into GenericColumn with correct type)
                |------ OPTIMIZED:  folder with optimized columns (first of all, Text - only unique text values plus hashmap)
                |------ INDEX:      folder with indices (in the future)
-}

module Quark.Base.Storage
    ( 
        saveGenColumn,
        loadGenColumn
        
    ) where

import Quark.Base.Column

import System.Directory

import Data.Binary
import Data.ByteString.Lazy as BL

import Data.Text

-- Generic column serialization
saveGenColumn :: FilePath -> GenericColumn -> IO ()
saveGenColumn file col = BL.writeFile file (encode col)

loadGenColumn :: FilePath -> IO GenericColumn
loadGenColumn file = BL.readFile file >>= return . decode

listFiles = getDirectoryContents






