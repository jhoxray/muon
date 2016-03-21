{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, 
                TypeSynonymInstances, FlexibleInstances, OverloadedLists, DeriveGeneric, BangPatterns, FlexibleContexts  #-}

{-
    Aggregation functions
-}

module Quark.Base.Aggregation
    ( 
        groupColumnsGen,
        groupColumns,
        groupColumns2,
        groupColumns3,
        groupColumnsST,
        groupColumnsH,
        groupColumnsFList,
        groupColumnsG1A1,
        groupColumnsG1A2,
        groupColumnsG1A3,
        groupColumnsG2A2,
        groupColumnsG3A2,
        groupColumnsG2A3,
        groupColumnsG3A3
        

    ) where

import Quark.Base.Column
import Quark.Base.Raw

import qualified Data.HashMap.Strict as Map
import qualified Data.Vector.Generic as G

import Data.Hashable
import Data.Int

import Control.Monad.ST
import Data.STRef

import System.IO

import Data.Tuple.Select (sel1)

import qualified Data.List as L

-- import qualified Data.HashTable.ST.Cuckoo as C
-- import qualified Data.HashTable.Class as H
-- type HashTable s k v = C.HashTable s k v

import qualified Data.HashTable.IO as H
type HashTable k v = H.LinearHashTable k v





-- groupAggregate (x:[]) = groupColumns x
-- groupAggregate (x1:x2:[]) = groupColumns2 (x1,x2) 

groupColumnsG1A1 xxs f g yys = G.ifoldl' (\ !acc i x -> Map.insertWith f x (g $ yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumnsG1A1 #-}    

-- pattern GnAm - group by n cols aggregate by m cols
-- what to do about boilerplate?????? --> see zipFTuples and mkGTuple helper functions
{-
groupColumnsG1A2 xs f g ys f1 g1 ys1 = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 (f,f1)) x (mkGTuple2 (g, g1) (ys, ys1) i) acc) Map.empty xs 
-}        
-- changing signature to use tuples: ft = (f1, f2), gt = (g1, g2), yt = (y1, y2), xt = (x1, x2, ...) etc
-- this set of functions takes set of tuples: 
--      xt - columns to group by
--      yt - columns to aggregate by
--      gt - functions applied to each member of yt before applying ft
--      ft - aggregation functions
-- So, groupby col1, col2 aggregate sum col3, count col4 will be transformed to:
-- groupColumnsG2A2 (col1, col2) ((+), (+)) (id, const 1) (col3, col4)
-- (as sum == (+) id while count == (+) (const 1))
groupColumnsG1A2 xs ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) x (mkATuple2 gt yt i) acc) Map.empty xs 
{-# INLINE groupColumnsG1A2 #-}  

groupColumnsG1A3 xs ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) x (mkATuple3 gt yt i) acc) Map.empty xs 
{-# INLINE groupColumnsG1A3 #-}  

groupColumnsG2A2 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) (mkGTuple2 xt i) (mkATuple2 gt yt i) acc) Map.empty (fst xt) 
{-# INLINE groupColumnsG2A2 #-}  

groupColumnsG2A3 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) (mkGTuple2 xt i) (mkATuple3 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG2A3 #-}  

groupColumnsG3A2 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples2 ft) (mkGTuple3 xt i) (mkATuple2 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG3A2 #-}  

groupColumnsG3A3 xt ft gt yt = 
    G.ifoldl' (\ !acc i x -> Map.insertWith (zipFTuples3 ft) (mkGTuple3 xt i) (mkATuple3 gt yt i) acc) Map.empty (sel1 xt)
{-# INLINE groupColumnsG3A3 #-}  

{-# INLINE zipFTuples2 #-}  
zipFTuples2 (f1, f2) (x1,x2) (y1,y2) = (f1 x1 y1, f2 x2 y2)
{-# INLINE zipFTuples3 #-}  
zipFTuples3 (f1, f2, f3) (x1,x2, x3) (y1,y2, y3)= (f1 x1 y1, f2 x2 y2, f3 x3 y3)

{-# INLINE mkATuple2 #-}  
mkATuple2 (g1, g2) (y1, y2) i = (g1 $ y1 G.! i, g2 $ y2 G.! i)
{-# INLINE mkATuple3 #-}  
mkATuple3 (g1, g2, g3) (y1, y2, y3) i = (g1 $ y1 G.! i, g2 $ y2 G.! i, g3 $ y3 G.! i)

{-# INLINE mkGTuple2 #-}  
mkGTuple2 (x1, x2) i = (x1 G.! i, x2 G.! i)
{-# INLINE mkGTuple3 #-}  
mkGTuple3 (x1, x2, x3) i = (x1 G.! i, x2 G.! i, x3 G.! i)



{-# INLINE groupColumnsFList #-}
-- works, but has obvious limitations (aggregating columns need to be of the same type)
groupColumnsFList ::
  (Eq k, G.Vector v a1, G.Vector v1 k, Hashable k) =>
  v1 k -> [a -> a -> a] -> [a1 -> a] -> [v a1] -> Map.HashMap k [a]
groupColumnsFList xxs fs gs yys = 
    let pline !acc i x = Map.insertWith func x (L.zipWith ($) gs (L.map (G.! i) yys)) acc
        func = L.zipWith3 ($) fs
    in G.ifoldl' pline Map.empty xxs



-- group by 1 column, with only 1 aggregation function f to be used on column yys
groupColumns :: forall k v v1 v2. (Eq k, G.Vector v k, G.Vector v2 v1, Hashable k) =>
     v k -> (v1 -> v1 -> v1) -> v2 v1 -> Map.HashMap k v1
groupColumns xxs f yys = G.ifoldl' (\ !acc i x -> Map.insertWith f x (yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumns #-}    

groupColumnsST xxs f yys = ifoldlST (\ !acc i x -> Map.insertWith f x (yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumnsST #-}    

-- group by 2 columns, with only 1 aggregation function
groupColumns2 (x1,x2) f ws = G.ifoldl' (\ !acc i x -> Map.insertWith f (x, (x2 G.! i)) (ws G.! i) acc) (Map.fromList []) x1
{-# INLINE groupColumns2 #-}
-- group by 3 columns, with only 1 aggregation function -- etc...
groupColumns3 (x1,x2,x3) f ws = G.ifoldl' (\ !acc i x -> Map.insertWith f (x, (x2 G.! i), (x3 G.! i)) (ws G.! i) acc) (Map.fromList []) x1 
{-# INLINE groupColumns3 #-}

{-# INLINE groupColumnsH #-}    
groupColumnsH :: (Eq k, G.Vector v k, G.Vector v1 v2, Hashable k) =>
    v k -> (v2 -> v2 -> v2) -> v1 v2 -> IO (HashTable k v2)
groupColumnsH xs f ys = do 
    ht <- H.new 
    flip G.imapM_ xs $ \i x -> do  
        let !y = ys G.! i
        acc <- H.lookup ht x
        case acc of 
            Just v -> H.insert ht x (f y v)
            Nothing -> H.insert ht x y
    return ht

groupColumns'' :: GenericVector -> (forall a. a->a->a) -> GenericVector -> IO()
groupColumns'' (GenericVector v) f (GenericVector w) = 
    do let res = groupColumns v f w
       putStrLn $ show res

groupColumns' :: GenericVector -> (forall a. Num a => a->a->a) -> NumericVector -> IO()
groupColumns' (GenericVector v) f (NumericVector w) = 
    do let res = groupColumns v f w
       putStrLn $ show res

{- right fold performs worse
groupColumns3R (x,y,z) f ws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (y G.! i), (z G.! i)) (ws G.! i) acc) (Map.fromList []) x 
-}


-- group by (x:xs) - list of columns to groupby, with only 1 aggregation function f to be used on column ws
-- this one is VERY SLOW - because of PRelude??
groupColumnsGen f (x:xs) ws = 
    G.ifoldl' pline (Map.fromList []) x
    where 
        pline acc i l = Map.insertWith f (l: (Prelude.map (G.! i) xs)) (ws G.! i) acc
{-# INLINE groupColumnsGen #-}     


vfold f z xxs yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) z xxs 

vfold2 f z xxs yys wws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 
vfoldr2 f z xxs yys wws = G.ifoldr' (\i x acc -> Map.insertWith f (x, (yys G.! i)) (wws G.! i) acc) z xxs 


-- foldlST :: (a -> b -> a) -> a -> [b] -> a
foldlST f acc xs = runST $ do
    acc' <- newSTRef acc            -- Create a variable for the accumulator
 
    G.forM_ xs $ \x -> do             -- For each x in xs...
 
        a <- readSTRef acc'         -- read the accumulator
        writeSTRef acc' (f a x)     -- apply f to the accumulator and x
 
    readSTRef acc'                  -- and finally read the result


{-# INLINE ifoldlST #-}
ifoldlST f acc xs = runST $ do
    acc' <- newSTRef acc            -- Create a variable for the accumulator
 
    flip G.imapM_ xs $ \i x -> do             -- For each x in xs...
 
        a <- readSTRef acc'         -- read the accumulator
        writeSTRef acc' (f a i x)     -- apply f to the accumulator and x
 
    readSTRef acc'                  -- and finally read the result
