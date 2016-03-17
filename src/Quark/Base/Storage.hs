{-# LANGUAGE OverloadedStrings, RankNTypes, TypeSynonymInstances, 
    FlexibleInstances, OverloadedLists, DeriveGeneric, DeriveAnyClass  #-}

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
        loadGenColumn,
        createTableMetadata,
        saveCTable,
        loadCTable
        -- saveColumn,
        -- loadColumn
    ) where

import Quark.Base.Column

import System.Directory -- directory manipulations

import Data.Binary
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector.Generic as G

import qualified Data.HashMap.Strict as Map
import Data.Hashable

import Data.Text

import GHC.Generics (Generic)

data TableMetadata = TableMetadata { rawColumnsMeta :: [(Text, FilePath)] -- name of the column and full link to the column file
                     } deriving(Show, Generic)

instance Binary TableMetadata


-- pure function that creates TableMetadata from a given Table and table directory path
createTableMetadata :: CTable           -- CTable
                    -> FilePath         -- system directory (Now, FLAT - so we put everything in this dir)
                    -> TableMetadata    -- resulting metadata
createTableMetadata ct fp = 
    TableMetadata {rawColumnsMeta = fst $ Map.foldlWithKey' proc ([], 0) ct}
    where proc (lst, i) k v = ( (k, fp ++ "/column" ++ show i) : lst, i + 1)


-- | save a CTable - starting with only RAW columns
saveCTable :: CTable        -- ^ CTable to save
           -> FilePath      -- ^ System Directory path
           -> IO ()     
saveCTable table sysDir = 
    do let meta = createTableMetadata table sysDir
       BL.writeFile (sysDir ++ "/metadata") (encode meta) -- saving metadata first
       mapM_ proc (rawColumnsMeta meta) -- processing columns
       where proc (n, fp) = 
                do  let col = Map.lookup n table
                    case col of
                        Just c      -> saveGenColumn fp c
                        Nothing     -> return ()


-- | load a CTable previously saved with saveCTable
loadCTable :: FilePath          -- ^ directory with table metadata
           -> IO CTable         -- ^ resulting CTable with loaded columns (how to handle errors?)
loadCTable fp = 
    do metafile <- BL.readFile (fp ++ "/metadata")      -- reading metadata file from a given folder
       let meta = decode metafile :: TableMetadata      -- decoding metadata file
       ls <- mapM proc (rawColumnsMeta meta)            -- loading all columns in order
       return (Map.fromList ls)                         -- returning CTable
       where proc (n, fpath) = 
                do gc <- loadGenColumn fpath
                   return (n, gc)



-- Generic column serialization
saveGenColumn :: FilePath -> GenericColumn -> IO ()
saveGenColumn file col = BL.writeFile file (encode col)

loadGenColumn :: FilePath -> IO GenericColumn
loadGenColumn file = BL.readFile file >>= return . decode

listFiles = getDirectoryContents


{-
saveColumn :: (G.Vector v a, Binary (v a )) => FilePath -> Column v a -> IO ()
saveColumn file col = BL.writeFile file (encode col)

loadColumn :: (G.Vector v a, Binary (v a )) => FilePath -> IO (Column v a)
loadColumn file = BL.readFile file >>= return . decode
-}




