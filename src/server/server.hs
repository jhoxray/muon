{-# LANGUAGE OverloadedStrings #-}

import Network.WebSockets
import Data.Aeson

main :: IO ()
main = runServer "127.0.0.1" 8080 handleConnection

handleConnection pending = do
    print "Server listening on port 8080"
    connection <- acceptRequest pending
    print "Connected!"
    let loop = do
            cmdMsg <- receiveDataMessage connection
            print $ "Received message: " ++ (show cmdMsg)
            print $ decodeDM cmdMsg
            let rmsg = Text "Received Message -->"
            sendDataMessage connection rmsg
            sendDataMessage connection cmdMsg
            loop
    loop


decodeDM :: DataMessage -> Maybe Value
-- DataMessage can be Text s and Binary s
decodeDM (Text s) = decode s
decodeDM _ = Nothing