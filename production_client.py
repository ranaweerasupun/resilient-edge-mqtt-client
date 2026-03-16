import paho.mqtt.client as mqtt
import time
import threading
from inflight_tracker import InflightTracker
from offline_queue import OfflineQueue

class ProductionMQTTClient:
    """
    Production-ready MQTT client with offline queuing and inflight tracking.
    
    This client handles all the edge cases:
    - Automatic reconnection with exponential backoff
    - Offline message queuing when disconnected
    - Inflight message tracking for reliable QoS 1/2 delivery
    - Priority-based queue management
    - Graceful degradation under constrained resources
    """
    
    def __init__(self, client_id, broker_host="localhost", broker_port=1883,
                 max_queue_size=1000):
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        
        # Connection state tracking
        self.is_connected = False
        self.connection_lock = threading.Lock()
        
        # Reconnection backoff parameters
        self.min_backoff = 1
        self.max_backoff = 60
        self.current_backoff = self.min_backoff
        self.reconnect_in_progress = False
        
        # Initialize storage systems
        self.inflight_tracker = InflightTracker()
        self.offline_queue = OfflineQueue(max_size=max_queue_size)
        
        # Create MQTT client
        self.client = mqtt.Client(client_id=client_id, clean_session=False)
        
        # Set up callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        # Background thread for draining offline queue
        self.queue_drainer_running = False
        self.queue_drainer_thread = None
    
    def _on_connect(self, client, userdata, flags, rc):
        """Handle successful connection."""
        if rc == 0:
            with self.connection_lock:
                self.is_connected = True
            
            print(f"✓ Connected to broker (session_present={flags.get('session present', False)})")
            
            # Reset backoff on successful connection
            self.current_backoff = self.min_backoff
            self.reconnect_in_progress = False
            
            # First, resend any inflight messages from previous connection
            self._resend_inflight_messages()
            
            # Then, start draining the offline queue
            self._start_queue_drainer()
        else:
            print(f"✗ Connection failed with code {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection."""
        with self.connection_lock:
            self.is_connected = False
        
        # Stop the queue drainer since we're disconnected
        self._stop_queue_drainer()
        
        if rc == 0:
            print("Disconnected cleanly from broker")
        else:
            print(f"✗ Unexpected disconnection (code {rc})")
            self._reconnect_with_backoff()
    
    def _on_publish(self, client, userdata, mid):
        """Handle message acknowledgment."""
        print(f"✓ Message {mid} acknowledged by broker")
        self.inflight_tracker.remove_message(mid)
    
    def _resend_inflight_messages(self):
        """Resend messages that were inflight during previous connection."""
        messages = self.inflight_tracker.get_all_messages()
        
        if not messages:
            return
        
        print(f"⟳ Resending {len(messages)} inflight messages...")
        
        for msg in messages:
            info = self.client.publish(
                topic=msg['topic'],
                payload=msg['payload'],
                qos=msg['qos'],
                retain=msg['retain']
            )
            print(f"  ⟳ Resent: {msg['topic']}")
    
    def _start_queue_drainer(self):
        """
        Start background thread that drains the offline queue.
        
        This runs continuously while connected, pulling messages from
        the offline queue and publishing them. It processes messages in
        batches for efficiency.
        """
        if self.queue_drainer_running:
            return
        
        self.queue_drainer_running = True
        
        def drain_queue():
            print("🔄 Queue drainer started")
            
            while self.queue_drainer_running and self.is_connected:
                # Get next batch of messages
                messages = self.offline_queue.get_next_batch(batch_size=10)
                
                if not messages:
                    # Queue is empty, wait a bit before checking again
                    time.sleep(1)
                    continue
                
                print(f"📤 Draining {len(messages)} messages from offline queue...")
                
                for msg in messages:
                    if not self.is_connected:
                        # Lost connection while draining, stop
                        print("⚠ Connection lost during queue drain")
                        break
                    
                    # Publish the message
                    info = self.client.publish(
                        topic=msg['topic'],
                        payload=msg['payload'],
                        qos=msg['qos'],
                        retain=msg['retain']
                    )
                    
                    # For QoS 1/2, add to inflight tracker
                    if msg['qos'] > 0:
                        self.inflight_tracker.add_message(
                            packet_id=info.mid,
                            topic=msg['topic'],
                            payload=msg['payload'],
                            qos=msg['qos'],
                            retain=msg['retain']
                        )
                    
                    # Remove from offline queue
                    self.offline_queue.remove_message(msg['id'])
                    
                    print(f"  📤 Published: {msg['topic']}")
                    
                    # Small delay between messages to avoid overwhelming broker
                    time.sleep(0.1)
            
            print("🔄 Queue drainer stopped")
        
        self.queue_drainer_thread = threading.Thread(target=drain_queue, daemon=True)
        self.queue_drainer_thread.start()
    
    def _stop_queue_drainer(self):
        """Stop the queue drainer thread."""
        self.queue_drainer_running = False
        if self.queue_drainer_thread:
            self.queue_drainer_thread.join(timeout=2)
    
    def _reconnect_with_backoff(self):
        """Attempt to reconnect with exponential backoff."""
        if self.reconnect_in_progress:
            return
        
        self.reconnect_in_progress = True
        
        def reconnect_thread():
            while self.reconnect_in_progress:
                print(f"⟳ Waiting {self.current_backoff}s before reconnection attempt...")
                time.sleep(self.current_backoff)
                
                try:
                    print("⟳ Attempting to reconnect...")
                    self.client.reconnect()
                    break
                except Exception as e:
                    print(f"✗ Reconnection failed: {e}")
                    self.current_backoff = min(self.current_backoff * 2, self.max_backoff)
        
        thread = threading.Thread(target=reconnect_thread, daemon=True)
        thread.start()
    
    def connect(self):
        """Establish initial connection to the broker."""
        print(f"Connecting to {self.broker_host}:{self.broker_port}...")
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            print(f"Initial connection failed: {e}")
            self._reconnect_with_backoff()
    
    def publish(self, topic, payload, qos=0, retain=False, priority=1):
        """
        Publish a message with intelligent routing.
        
        If connected: publish directly and track with inflight tracker for QoS 1/2
        If disconnected: add to offline queue for later delivery
        
        The priority parameter (1-10, higher is more important) determines
        which messages get dropped first if the offline queue fills up.
        """
        with self.connection_lock:
            connected = self.is_connected
        
        if connected:
            # We're connected - publish normally
            info = self.client.publish(topic, payload, qos, retain)
            
            # Track for QoS 1/2
            if qos > 0:
                self.inflight_tracker.add_message(
                    packet_id=info.mid,
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain
                )
            
            return info
        else:
            # We're disconnected - add to offline queue
            success = self.offline_queue.add_message(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                priority=priority
            )
            
            if not success:
                print(f"⚠ Failed to queue message - queue full and couldn't make room")
            
            return None
    
    def get_statistics(self):
        """
        Get comprehensive statistics about the client state.
        
        This is useful for monitoring and debugging. You'd typically
        expose these through a health check endpoint or monitoring dashboard.
        """
        queue_stats = self.offline_queue.get_stats()
        inflight_count = self.inflight_tracker.count_messages()
        
        return {
            'connected': self.is_connected,
            'current_backoff_seconds': self.current_backoff,
            'offline_queue': queue_stats,
            'inflight_messages': inflight_count
        }
    
    def start(self):
        """Start the network loop."""
        self.client.loop_start()
    
    def stop(self):
        """Stop the network loop and close connections."""
        self.reconnect_in_progress = False
        self._stop_queue_drainer()
        self.client.loop_stop()
        self.inflight_tracker.close()
        self.offline_queue.close()
