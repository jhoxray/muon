{-# LANGUAGE OverloadedStrings, RankNTypes, GADTs, OverloadedLists  #-}



{-
    Column datastore approach
-}

module Quark.Base.Raw
    ( 
        ColumnMemoryStore(..),
        emptyCMS,
        
        getColNames,
        checkColType,
        checkColType',
        
        getIntColumn,
        getDoubleColumn,
        getTextColumn,

        cTableToCMS
    ) where


import Data.Int
import Data.Word

import qualified Data.Vector.Unboxed as U
import qualified Data.Vector as V
import Data.Vector.Generic as G

import Data.Text
import Data.Time
import Data.Time.Calendar
-- import Text.Read
import Data.Text.Read
import Data.Binary

-- import qualified Data.Map.Strict as Map
import qualified Data.HashMap.Strict as Map
import Data.Hashable

import Control.Monad (mapM_)

import Data.Vector.Binary -- Binary instances for Vectors!!! 
-- So, primitive serialization for unboxed types (suitable for not very large files) should be covered automatically
-- Actually, Text is Binary so works automagically as well! Supah! :)

import Quark.Base.Data
import Quark.Base.Column

type UColMap a = Map.HashMap Text (U.Vector a)
type BColMap a = Map.HashMap Text (V.Vector a)

-- Ok, trying different approach -- storing all columns in maps by type 
data ColumnMemoryStore = ColumnMemoryStore {
        intCols :: UColMap Int64,
        doubleCols :: UColMap Double,
        textCols :: BColMap Text,
        typeSchema :: Map.HashMap Text SupportedTypes -- helper map from column names to their types
    
    } deriving (Show)

emptyCMS = ColumnMemoryStore {intCols = Map.empty, doubleCols = Map.empty, textCols = Map.empty, typeSchema = Map.empty}

getColNames :: ColumnMemoryStore -> [Text]
getColNames cms = Map.keys (typeSchema cms)

-- checks type of the column and returns Maybe SupportedType - useful to check if the column is in the store
checkColType :: Text -> ColumnMemoryStore -> Maybe SupportedTypes
checkColType nm cms = Map.lookup nm (typeSchema cms)

-- use this if you are sure column is in the store, it's incomplete (returns nothing for Nothing) so - DANGEROUS!!!
checkColType' :: Text -> ColumnMemoryStore -> SupportedTypes
checkColType' nm cms = 
    let c = checkColType nm cms
    in case c of
        Just t -> t

-- return any unboxed columns - specific definitions below
getUColumn :: U.Unbox a => Text -> UColMap a -> U.Vector a
getUColumn nm cms = 
    let mclm = Map.lookup nm cms
    in case mclm of
            Just v -> v
            Nothing -> U.fromList []

getIntColumn nm cms = getUColumn nm (intCols cms)
getDoubleColumn nm cms = getUColumn nm (doubleCols cms)

-- return any boxed column - specific definitions below
getBColumn :: Text -> BColMap a -> V.Vector a
getBColumn nm cms = 
    let mclm = Map.lookup nm cms
    in case mclm of
            Just v -> v
            Nothing -> V.fromList []

getTextColumn nm cms = getBColumn nm (textCols cms)

-- compatibility conversion function
cTableToCMS :: CTable -> ColumnMemoryStore
cTableToCMS ct = 
    Prelude.foldl proc emptyCMS (Map.toList ct)
    where proc acc (k,v) = case v of
                                (CDouble c) -> acc {doubleCols = Map.insert k c (doubleCols acc), typeSchema = Map.insert k PDouble (typeSchema acc)}
                                (CInt c) -> acc { intCols = Map.insert k c (intCols acc), typeSchema = Map.insert k PInt (typeSchema acc)}
                                (CText c) -> acc {textCols = Map.insert k c (textCols acc), typeSchema = Map.insert k PText (typeSchema acc)}





























