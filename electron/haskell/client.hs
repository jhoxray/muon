{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving #-}

import Haste
import Control.Applicative

-- import HasteHighcharts
--same dir

import Haste.JSON
import Haste.Foreign
import Haste.DOM
import Haste.Prim
import Haste.Events

import Highcharts

newtype WebSocket = WebSocket JSAny deriving (ToAny, FromAny)

main = do
    
    ws <- newWS "ws://localhost:8080/"

    melm <- elemById "div1"
    -- let e1 = (fmap (getStyle :: Elem -> PropID -> IO String)  melm) -- <*> Just "margin-left"
    -- again, killed a lot of time trying to figure it out. with fmap - it only works with full type spec. with <$> - works without. WHY????
    -- problem is because elemById returns Maybe Elem

    let e2 = getStyle <$> melm <*> Just "margin-left"
    -- writeLog $ show cob
    case e2 of
        Nothing -> writeLog "Not found!"
        Just s -> writeLog "Got a property:" >> s >>= writeLog 

    Just btn <- elemById "btn"
    onEvent btn Click $ \_ -> sendS ws "Hello Server!!"

    let co = ChartOptions {renderTo = "div1", chartType = "line"}
    c1 <- newChart co
    return c1


consoleLog :: JSAny -> IO ()
consoleLog = ffi "(function(o) {console.log(o);})"

newWebSocket :: URL
    -> (WebSocket -> JSString -> IO ())
    -> (WebSocket -> IO ())
    -> IO ()
    -> IO WebSocket
newWebSocket = ffi "(function(url, cb, f, err) {\
             \var ws = new WebSocket(url);\
             \ws.onmessage = function(e) {cb(ws,e.data);};\
             \ws.onopen = function(e) {f(ws);};\
             \ws.onerror = function(e) {err());};\
             \return ws;\
           \})" 


newWS :: URL -> IO WebSocket
newWS = ffi "(function(url, cb, f, err) {\
             \var ws = new WebSocket(url);\
             \ws.onmessage = function(e) {console.log(e.data);};\
             \return ws;\
           \})" 

sendS :: WebSocket -> JSString -> IO ()
sendS = ffi "(function(s, msg) {s.send(msg);})"