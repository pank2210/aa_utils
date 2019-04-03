#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import time
import json

try:   
    from ws4py.client.threadedclient import WebSocketClient
except ImportError:
    raise Exception('ws4py not available. Please install it using: sudo pip install ws4py')

INIT_MESSAGE = {'command': 'vox_csr_init'}
OPEN_CHANNEL_MESSAGE = {'command': 'vox_csr_open_channel', 'language':'en-us', 'slm':'fst:postpaid_en', 'acousticModel': 'verbio8k.en-us'}
CLOSE_CHANNEL_MESSAGE = {'command': 'vox_csr_close', 'value':''}

channel_ready = False

class CSRWebsocketClient(WebSocketClient):
    """ Simple WebSocketClient class """
    def initialize(self, opened_cb, closed_cb, result_cb, connection_id=0):
        self._opened_cb = opened_cb
        self._closed_cb = closed_cb
        self._result_cb = result_cb
        self._cid = connection_id
        self.isOpened = False

    def opened(self):
        self.isOpened = True
        if self._opened_cb:
            self._opened_cb(self._cid)


    def closed(self, code, reason=None):
        print("closed [%s]" % (str(reason)))
        if self._closed_cb:
            self._closed_cb(str(reason))
        self.isOpened = False

    def received_message(self, m):
        print("received_message: %s" % str(m))
        if self._result_cb:
            self._result_cb(m)

    def send_audio(self, audio_buffer):
        if not audio_buffer:
            return

        #print("audio_buffer of %d bytes" % len(audio_buffer))
        try:
            self.send(audio_buffer, binary=True)
        except Exception, e:
            print("Exception sending audio: %s" % str(e))

    def send_text(self, msg):
        if not msg:
            return

        try:
            print("Sending %s" % str(msg))
            self.send(msg)
        except Exception, e:
            print("Exception sending text message: %s" % str(e))

    def closeAndWait(self):
        self.close();
        self.waitUnilClosed();

    def waitUnilClosed(self):
        for i in xrange(600):
            time.sleep(0.1)
            if self.isOpened is False:
                    time.sleep(0.5);
                    return;
        print("Timeout when waiting for channel to be closed.");



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Sample Websocket Gateway client')
    parser.add_argument('--audio-file', help='audio file to process', required=True)
    parser.set_defaults(feature=False)
    args = parser.parse_args()

    audio_file=args.audio_file
    print('audio_file: %s' % audio_file)

    buffer_size = 16000
    
    global channel_ready

    def stream_audio_to_server(audio_file, csr_ws):
        with open(audio_file, 'rb') as w:
            w.read(44)
            audio_buffer = w.read(buffer_size)
            while audio_buffer:
                csr_ws.send_audio(audio_buffer)
                audio_buffer = w.read(buffer_size)
                time.sleep(0.9)
    def csr_ws_opened(cid):
        # Once the connection has been stablished, we can send the init message.
        # We will receive a response to this init message in csr_ws_received_message function.
        csr_ws.send_text(json.dumps(INIT_MESSAGE))

    def csr_ws_closed(reason):
        print('csr_ws_closed %s' % reason)

    def csr_ws_received_message(message):
       m = json.loads(str(message))
       global channel_ready
       global stream_ready
       if ('vox_csr_init' in m) and (m['vox_csr_init'] == 'success'):
            # Once we receive a successful response to our vox_csr_init message, we can 
            # open a recognition channel sending a vox_csr_open_channel message.
            csr_ws.send_text(json.dumps(OPEN_CHANNEL_MESSAGE))
       elif ('vox_csr_open_channel' in m) and (m['vox_csr_open_channel'] == 'success'):
            # If we receive a successful response to our vox_csr_open_channel message
            print('Setting channel_ready to True. Engine ready to receive an audio stream.')
            channel_ready = True
       elif ('vox_csr_channel_closed' in m):
            print ('Audio stream channel closed')
            channel_ready = False
       elif ('vox_csr_closed' in m):
            print('Dev channel closed')
            channel_ready = False

    csr_ws = CSRWebsocketClient('ws://127.0.0.1:3000', protocols=['http-only', 'chat'])
    csr_ws.initialize(csr_ws_opened, csr_ws_closed, csr_ws_received_message)
    csr_ws.connect()

    # For simplicity, we don't use a sepparate thread to stream our audio file.
    # We just wait for the channel_ready flag (set to True when receiving a successful vox_csr_open_channel response)
    while not channel_ready:
        print('Waiting for channel...')
        time.sleep(0.1)
    
    print('Channel ready. Start streaming audio to server.')
    stream_audio_to_server(audio_file, csr_ws)

    # Once all audio data has been sent to the server, we can close the streaming channel
    csr_ws.send_text(json.dumps(CLOSE_CHANNEL_MESSAGE))
    time.sleep(1)                

    for i in xrange(6000):
        print("Waiting for audio channel to be closed ...")
        time.sleep(0.5)
        if not channel_ready:
            break

    time.sleep(1)

    csr_ws.closeAndWait()

    time.sleep(1);


