{-# LANGUAGE OverloadedStrings, RankNTypes, GADTs, OverloadedLists, 
MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, StandaloneDeriving, DeriveDataTypeable  #-}



{-
    Column datastore approach
-}

module Quark.Base.Raw
    ( 
        ColumnMemoryStore(..),
        VPair(..),
        NumericVector(..),
        GenericVector(..),
        emptyCMS,
        
        getColNames,
        getColNamesTypes,
        checkColType,
        checkColType',

        extractVec,
        
        
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

import Data.Typeable
import Data.Data
import Data.Dynamic


import Data.Vector.Binary -- Binary instances for Vectors!!! 
-- So, primitive serialization for unboxed types (suitable for not very large files) should be covered automatically
-- Actually, Text is Binary so works automagically as well! Supah! :)

import Quark.Base.Data
import Quark.Base.Column

type UColMap a = Map.HashMap Text (U.Vector a)
type BColMap a = Map.HashMap Text (V.Vector a)

data VPair where
    MkHM :: (Hashable k, Show k, Show v) => Map.HashMap k v -> VPair
    MkVec ::  (Eq a1, Show a1, Hashable a1, Vector v1 a1, Show (v1 a1) ) => v1 a1 -> VPair
    MkPair2 :: (Vector v1 a1, Vector v2 a2) => v1 a1 -> v2 a2 -> VPair
    MkPair3 :: (Vector v1 a1, Vector v2 a2, Vector v3 a3) => v1 a1 -> v2 a2 -> v3 a3 -> VPair
    MkPair4 :: (Vector v1 a1, Vector v2 a2, Vector v3 a3, Vector v4 a4) => v1 a1 -> v2 a2 -> v3 a3 -> v4 a4 -> VPair

data NumericVector where
    NumericVector :: (Num a, Show a, U.Unbox a) => U.Vector a -> NumericVector

data GenericVector where
    GenericVector :: (Show a, Eq a, Hashable a, Vector v a, Show (v a)) => v a -> GenericVector

-- numToGen (NumericVector n) = GenericVector n

-- genToNum (GenericVector  (U.Vector a) ) = NumericVector (U.Vector a)


data Type a where
  TBool :: Type Bool
  TInt  :: Type Int

def :: Type a -> a
def TBool = False
def TInt  = 0


deriving instance Show GenericVector 
deriving instance Show NumericVector

instance Show VPair where
    show (MkVec v) = show v
    show (MkHM hm) = show hm

-- Ok, trying different approach -- storing all columns in maps by type 
data ColumnMemoryStore = ColumnMemoryStore {
        intCols :: UColMap Int64,
        doubleCols :: UColMap Double,
        textCols :: BColMap Text,
        typeSchema :: Map.HashMap Text SupportedTypes -- helper map from column names to their types
    
    } deriving (Show)

data HColumn = HColumn {
        intCol :: U.Vector Int64,
        doubleCol :: U.Vector Double,
        accessor ::  HColumn -> (forall v a. (Vector v a, Num a) => v a)
    }   

data SomeVec = forall v a . (Typeable v, Typeable a, Show (v a)) => SomeVec (v a) deriving (Typeable)
-- deriving instance Data SomeVec
deriving instance Show SomeVec 

data HVec = HVec {
        dynVec :: Dynamic,
        castVec :: Dynamic -> (forall v a. (Vector v a, Num a, Typeable v, Typeable a, Show (v a)) => Maybe (v a))
    }



{-
data ColumnMemoryStoreRaw = forall a. (Num a, Show a, U.Unbox a) => CMSR (Map.HashMap Text (U.Vector a) )

cmsrInsert :: Text -> (forall a. (Num a, Show a, U.Unbox a) => U.Vector a )-> ColumnMemoryStoreRaw -> ColumnMemoryStoreRaw
cmsrInsert k v (CMSR m) = CMSR $ Map.insert k v m

cmsrLookup :: Text -> ColumnMemoryStoreRaw -> Maybe NumericVector 
cmsrLookup k (CMSR m) = case Map.lookup k m of
                            Just x -> Just $ NumericVector x
                            Nothing -> Nothing
-}

-- nGroupCols :: NumericVector -> (forall a. Num a => a -> a -> a) -> NumericVector

{-
extractVec' :: forall v a. Vector v a => Text -> ColumnMemoryStore -> v a
extractVec' nm cms = 
    let t = checkColType' nm cms
    in case t of
            PInt -> getUColumn nm (intCols cms)
-}

-- Ok, this class is what we need to extract any kind of Vectors from our storage by telling the type of what we need
class (Vector v a, Eq a, Hashable a) => ExV t v a where 
    extractVec :: Text -> t -> v a

instance ExV ColumnMemoryStore U.Vector Int64 where
    extractVec = getIntColumn

instance ExV ColumnMemoryStore U.Vector Double where
    extractVec = getDoubleColumn

instance ExV ColumnMemoryStore V.Vector Text where
    extractVec = getTextColumn

instance ExV (UColMap Int64) U.Vector Int64 where
    extractVec  = getUColumn 

instance ExV (UColMap Double) U.Vector Double where
    extractVec  = getUColumn 

instance ExV (BColMap Text) V.Vector Text where
    extractVec  = getBColumn

emptyCMS = ColumnMemoryStore {intCols = Map.empty, doubleCols = Map.empty, textCols = Map.empty, typeSchema = Map.empty}

getColNames :: ColumnMemoryStore -> [Text]
getColNames cms = Map.keys (typeSchema cms)

getColNamesTypes :: ColumnMemoryStore -> [(Text, SupportedTypes)]
getColNamesTypes cms = Map.toList (typeSchema cms)

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





























