{-# LANGUAGE OverloadedStrings, OverloadedLists  #-}

{-
    Basic parsing of expressions.
    with "table" groupby "col1", "col2" aggregate sum "col3"
-}

module Quark.Parsers.Basic
    ( 
        Func(..),
        OpTerm(..),
        Vars(..),
        parseAggregation,
        colToNames,
        funcToFunction,
        funcToFunctions
        
    ) where

import Data.Attoparsec.Text
import Data.Text
import Data.Attoparsec.Combinator

import Control.Applicative 

data Func = Id | Sum | Count deriving (Show, Eq, Ord, Enum)

data OpTerm
  = With Text       -- with "table_name"
  | GroupBy [Vars]
  | Aggr [Vars]
  deriving (Show, Eq, Ord)

data Vars = Col Text     -- variable
          | Appl Func Text -- applying Func to variable
         
    deriving (Show, Eq, Ord)


colToName :: Vars -> Text
colToName (Col t) = t
colToName _ = ""

colToNames :: OpTerm -> [Text]
colToNames (GroupBy vs) = Prelude.map colToName vs

-- funcToFunction (Appl Id t) = (id, t)
funcToFunction (Appl Sum t) = ( (+), t)

funcToFunctions (Aggr lst) = Prelude.map funcToFunction lst

quotedString :: Parser Vars
quotedString = Col <$> quotedStringText
    
quotedStringText :: Parser Text
quotedStringText = skipSpace *> char '"' *> takeWhile1 (\a -> a /= '"') <* char '"'
-- ok this is some basic applicative coolness: we are skipping space, skipping ", reading what's next while KEEPING it's result, skipping " and discarding.
-- so, what's left to return is takeWhie1... result!!!

columnNames :: Parser [Vars]  
columnNames = quotedString `sepBy` (char ',')

singleColFunction :: Parser Func
singleColFunction = fsum <|>
                    fcount <|>
                    id <|>
                    impliedId
                    where fsum = string "sum" *> pure Sum
                          fcount = string "count" *> pure Count
                          id = string "id" *> pure Id
                          impliedId = lookAhead quotedStringText *> pure Id -- if the next value in line is "safa" we have Id function implied

funcArg :: Parser Vars
funcArg = skipSpace *> (liftA2 Appl) singleColFunction (skipSpace *> quotedStringText)

funcArgs :: Parser [Vars]
funcArgs = funcArg `sepBy` (char ',')

-- parsing groupby terms with flat column names
pGroupBy :: Parser OpTerm
pGroupBy = GroupBy <$> (skipSpace *> string "groupby" *> columnNames)

-- parsing aggregate terms with functions on column names
pAggr :: Parser OpTerm
pAggr = Aggr <$> (skipSpace *> string "aggregate" *> funcArgs)

-- parsing full groupby ... aggregate ... expression, returning groupby as fst and aggregate as snd
pFullAggr :: Parser (OpTerm, OpTerm)
pFullAggr = do
    x <- pGroupBy
    y <- pAggr
    return (x,y)

parseAggregation = parseOnly pFullAggr

{-
funcArg :: Parser Vars
funcArg = do
    f <- singleColFunction
    skipSpace
    v <- quotedStringText
    return $ Appl f v

quotedStringText :: Parser Text
quotedStringText = do
    skipSpace
    char '"'
    e <- takeWhile1 (\a -> a /= '"')
    char '"'
    -- skipSpace
    return e


-}







