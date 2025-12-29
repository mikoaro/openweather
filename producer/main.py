import time
import os 
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.apiclient import WeatherAPIClient
from common.kafkaclient import KafkaClient
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv('WEATHER_KEY')
BASE_URL = os.getenv('base_url', 'http://api.openweathermap.org/data/2.5/weather')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')  
LOCATIONS_CONFIG_PATH = os.path.join(os.path.dirname(__file__),'../config/locations.json')
PRODUCER_SLEEP_SECONDS = int(os.getenv('PRODUCER_SLEEP_SECONDS', 300))  # ✅ ALTERADO: 5 minutos padrão
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'dados_brutos')

def load_locations(path):
    try: 
        with open(path, 'r', encoding='utf-8') as file:
            locations = json.load(file)
            logger.info(f"{len(locations)} localizações carregadas de {path}.")
            return locations
    except FileNotFoundError:
        logger.error(f"Arquivo de configuração não encontrado: {path}")
        return []
    except json.JSONDecodeError:
        logger.error(f"Erro ao decodificar o arquivo JSON: {path}")
        return []
    
def validate_enviroment():
    required_vars = {
        'WEATHER_KEY': API_KEY,
        'KAFKA_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS,
        'KAFKA_TOPIC': KAFKA_TOPIC
    }
    missing = [var for var, val in required_vars.items() if not val]
    if missing:
        logger.error(f"Variáveis de ambiente ausentes: {', '.join(missing)}")
        return False
    return True
    
def main():
    logger.info("Iniciando Weather Data Producer")
    logger.info(f"Intervalo de coleta: {PRODUCER_SLEEP_SECONDS} segundos")
    
    if not validate_enviroment():
        logging.error("Encerrado por configurações faltantes")
        return
    
    locations = load_locations(LOCATIONS_CONFIG_PATH)
    if not locations:
        logger.info("AVISO: Nenhum local para processar. Verifique o arquivo 'config/locations.json'. Encerrando o programa.")
        return
    
    try:    
        weather_client = WeatherAPIClient(API_KEY, BASE_URL)
        kafka_client = KafkaClient()
        kafka_producer = kafka_client.create_producer()
    except Exception as e:
        logger.error(f"Erro ao inicializar clientes: {e}")
        return
    
    try:
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Iteração #{iteration} - Coletando dados de {len(locations)} cidades")
            logger.info(f"{'='*60}")
            
            success_count = 0
            error_count = 0

            for loc in locations:
                try:
                    weather_data = weather_client.get_weather(lat=loc["lat"], lon=loc["lon"])
                    if not weather_data:
                        error_count += 1
                        continue
                    
                    # ✅ CORRIGIDO: Latitude e longitude agora vêm do config, não do weather_data
                    message = {
                        "city": weather_data.get('name', loc['name']),
                        "latitude": loc["lat"],
                        "longitude": loc["lon"],
                        "temperature_celsius": weather_data['main']['temp'],
                        "humidity": weather_data['main']['humidity'],
                        "pressure": weather_data['main']['pressure'],
                        "wind_speed": weather_data['wind']['speed'],
                        "timestamp_unix": weather_data.get('dt')
                    }

                    kafka_client.send_message(KAFKA_TOPIC, message)
                    logger.info(f"✅ {message['city']}: {message['temperature_celsius']}°C (lat: {message['latitude']}, lon: {message['longitude']})")
                    success_count += 1
                    
                except KeyError as e:
                    logger.error(f"Erro de estrutura de dados para {loc['name']}: {e}")
                    error_count += 1
                except Exception as e:
                    logger.error(f"Erro ao processar {loc['name']}: {e}")
                    error_count += 1
            
            logger.info(f"\n📊 Resumo da iteração #{iteration}:")
            logger.info(f"   ✅ Sucessos: {success_count}")
            logger.info(f"   ❌ Erros: {error_count}")
            logger.info(f"   ⏰ Próxima coleta em {PRODUCER_SLEEP_SECONDS} segundos")
            
            time.sleep(PRODUCER_SLEEP_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("\nProducer interrompido pelo usuário (Ctrl+C)")
    except Exception as e:
        logger.error(f"Erro fatal no producer: {e}")
    finally:
        kafka_client.close()
        logger.info("Producer finalizado")


if __name__ == "__main__":
    main()