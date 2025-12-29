import os
from supabase import create_client, Client
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY are required!")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info(f"Supabase client connected: {self.url}")
    
    def insert_data(self, data):
        """Inserts data into the weather_metrics table"""
        try:
            # Prepare the data
            record = {
                'city': data.get('city'),
                'latitude': data.get('latitude'),           # ✅ ADDED
                'longitude': data.get('longitude'),         # ✅ ADDED
                'temperatura_celsius': data.get('temperature_celsius'),
                'humidity': data.get('humidity'),
                'pressure': data.get('pressure'),
                'wind_speed': data.get('wind_speed'),
                'created_at': datetime.utcnow().isoformat()
            }
            
            # Insert into Supabase
            response = self.client.table('weather_metrics').insert(record).execute()
            
            # Check if you have successfully entered the information
            if response.data:
                logger.debug(f"✅ Data entered: {data.get('city')} (ID: {response.data[0].get('id')})")
                return True
            else:
                logger.warning(f"⚠️ Empty response upon insertion: {data.get('city')}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error inserting data into Supabase: {e}")
            return False
    
    def close(self):
        """Supabase does not need to close the connection (HTTP)"""
        logger.info("Supabase client finished")