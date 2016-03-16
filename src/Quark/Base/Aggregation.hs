{-# LANGUAGE OverloadedStrings, TransformListComp, RankNTypes, 
                TypeSynonymInstances, FlexibleInstances, OverloadedLists, DeriveGeneric  #-}

{-
    Aggregation functions
-}

module Quark.Base.Aggregation
    ( 
        groupColumnsGen,
        groupColumns,
        groupColumns2,
        groupColumns3
        

    ) where

import Quark.Base.Column
import qualified Data.HashMap.Strict as Map
import qualified Data.Vector.Generic as G


-- group by 1 column, with only 1 aggregation function f to be used on column yys
groupColumns xxs f yys = G.ifoldl' (\acc i x -> Map.insertWith f x (yys G.! i) acc) (Map.fromList []) xxs 
{-# INLINE groupColumns #-}    
-- group by 2 columns, with only 1 aggregation function
groupColumns2 (x1,x2) f ws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (x2 G.! i)) (ws G.! i) acc) (Map.fromList []) x1
{-# INLINE groupColumns2 #-}
-- group by 3 columns, with only 1 aggregation function -- etc...
groupColumns3 (x1,x2,x3) f ws = G.ifoldl' (\acc i x -> Map.insertWith f (x, (x2 G.! i), (x3 G.! i)) (ws G.! i) acc) (Map.fromList []) x1 
{-# INLINE groupColumns3 #-}


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

