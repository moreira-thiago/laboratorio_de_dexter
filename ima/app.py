import logging
from flask import Flask, request, jsonify, make_response
import pika
import mysql.connector
from flask_cors import CORS
from decouple import config
from datetime import datetime

app = Flask(__name__)

# Configurar o sistema de logs
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Habilitar o CORS globalmente
CORS(app, resources={r"/*": {"origins": "*"}})

# Configurações do RabbitMQ (Lembre-se de definir essas variáveis de ambiente em algum lugar)
rabbitmq_host = config("RABBITMQ_HOST")
rabbitmq_port = config("RABBITMQ_PORT")
rabbitmq_virtual_host = config("RABBITMQ_VIRTUAL_HOST")
rabbitmq_username = config("RABBITMQ_USER")
rabbitmq_password = config("RABBITMQ_PASSWORD")
rabbitmq_queue = config("RABBITMQ_QUEUE")

# Configurações do MySQL (Lembre-se de definir essas variáveis de ambiente em algum lugar)
mysql_host = config("DB_HOST")
mysql_port = config("DB_PORT")
mysql_user = config("DB_USER")
mysql_password = config("DB_PASSWORD")
mysql_database = config("DB_NAME")

# Função para conectar ao RabbitMQ
def conectar_rabbitmq():
    try:
        # Conexão com o RabbitMQ
        credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
        parameters = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, virtual_host=rabbitmq_virtual_host, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        return channel
    except Exception as e:
        logging.error("Erro ao conectar ao RabbitMQ: %s", str(e))
        return None

# Função para conectar ao MySQL
def conectar_mysql():
    try:
        connection = mysql.connector.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, database=mysql_database)
        return connection
    except Exception as e:
        logging.error("Erro ao conectar ao MySQL: %s", str(e))
        return None

# Lista de mensagens
mensagens_enviadas = []  # Vamos armazenar as mensagens aqui

# Rota para receber as solicitações da página web para enviar mensagens
@app.route('/enviar-mensagem', methods=['POST'])
def enviar_mensagem():
    try:
        data = request.json  # Assume que a página web envia JSON
        mensagem = data['mensagem']

        channel = conectar_rabbitmq()

        # Cria a fila se ela não existir
        channel.queue_declare(queue=rabbitmq_queue, durable=True)

        # Publica a mensagem na fila
        channel.basic_publish(exchange='', routing_key=rabbitmq_queue, body=mensagem, properties=pika.BasicProperties(
            delivery_mode=2,  # Torna a mensagem persistente
        ))

        # Encerra a conexão com o RabbitMQ
        channel.close()

        # Armazena a mensagem na lista
        mensagens_enviadas.append(mensagem)

        return jsonify({'message': 'Mensagem enviada com sucesso!'}), 200

    except Exception as e:
        logging.error("Erro ao enviar mensagem: %s", str(e))
        return jsonify({'error': str(e)}), 500

# Função para obter a contagem de mensagens na fila do RabbitMQ
def obter_contagem_fila():
    try:
        channel = conectar_rabbitmq()

        if channel is not None:
            queue_info = channel.queue_declare(queue=rabbitmq_queue, passive=True)

            # Retorna a contagem de mensagens na fila
            return queue_info.method.message_count
        else:
            logging.error("Erro ao conectar ao RabbitMQ")
            return -1
    except Exception as e:
        logging.error("Erro ao obter a contagem da fila do RabbitMQ: %s", str(e))
        return -1

# Rota para obter a contagem de mensagens na fila
@app.route('/contagem-fila', methods=['GET'])
def verificar_contagem_fila():
    contagem_fila = obter_contagem_fila()
    if contagem_fila >= 0:
        return jsonify({'contagem_fila': contagem_fila}), 200
    else:
        return jsonify({'error': 'Erro ao obter a contagem da fila'}), 500


def processar_fila(iniciar_processamento=False):
    global processando_fila  # Use a variável global

    try:
        if processando_fila and not iniciar_processamento:
            # Se o processamento da fila já estiver em andamento e o parâmetro `iniciar_processamento` for `False`, então retorne
            logging.info("O processamento da fila já está em andamento.")
            return

        processando_fila = True  # Marque que o processamento da fila está em andamento

        channel = conectar_rabbitmq()
        if channel:
            # Declara a fila
            result = channel.queue_declare(queue=rabbitmq_queue, durable=True, passive=True)
            num_mensagens_na_fila = result.method.message_count  # Obtém o número de mensagens na fila

            if num_mensagens_na_fila == 0:
                logging.info("A fila está vazia. Nenhuma mensagem para processar.")
                return

            # Itera sobre as mensagens na fila
            while num_mensagens_na_fila > 0:
                # Obtém a próxima mensagem da fila
                method, properties, body = channel.basic_get(queue=rabbitmq_queue, auto_ack=False)

                if method:
                    # Processa a mensagem
                    try:
                        mensagem = body.decode('utf-8')

                        # Extrai informações da mensagem
                        partes_mensagem = mensagem.split(', ')
                        data = partes_mensagem[0].split(': ')[1]
                        hora = partes_mensagem[1].split(': ')[1]
                        uuid = partes_mensagem[2].split(': ')[1]
                        nome = partes_mensagem[3].split(': ')[1]

                        # Processa a mensagem, por exemplo, salva no banco de dados
                        if salvar_mensagem_no_banco(data, hora, uuid, nome):
                            # Confirma o recebimento da mensagem se o processamento foi bem-sucedido
                            channel.basic_ack(delivery_tag=method.delivery_tag)
                        else:
                            # Rejeita a mensagem e ela será reenfileirada para processamento posterior
                            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    except Exception as e:
                        logging.error("Erro ao processar mensagem: %s", str(e))
                        # Rejeita a mensagem em caso de erro para que ela seja reenfileirada
                        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                num_mensagens_na_fila -= 1

        # Indica que o processamento da fila está concluído
        processando_fila = False
    except Exception as e:
        logging.error("Erro ao processar fila: %s", str(e))
    finally:
        processando_fila = False  # Marque que o processamento da fila terminou





# Rota para processar a fila e salvar mensagens no banco de dados
@app.route('/processar-fila', methods=['POST'])
def processar_fila_route():
    # Obtém o parâmetro `iniciar_processamento` da solicitação
    iniciar_processamento = request.get_json().get('iniciar_processamento', False)

    # Chama a função de processamento da fila
    processar_fila(iniciar_processamento)

    # Retorna uma resposta de sucesso
    return jsonify({'message': 'Processamento da fila iniciado com sucesso!'}), 200




# Função para salvar mensagem no banco de dados
def salvar_mensagem_no_banco(data, hora, uuid, nome):
    try:
        # Converter a data de 'DD/MM/AAAA' para 'AAAA-MM-DD'
        data_formatada = datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d')

        connection = conectar_mysql()
        if connection:
            cursor = connection.cursor()
            # Execute a inserção no banco de dados
            insert_query = "INSERT INTO tabela_mensagens (uuid, data, hora, nome) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (uuid, data_formatada, hora, nome))
            connection.commit()
            cursor.close()
            connection.close()
            logging.info("Mensagem salva no banco de dados: %s", nome)
        else:
            logging.error("Erro ao conectar ao MySQL")
    except Exception as e:
        logging.error("Erro ao salvar mensagem no banco de dados: %s", str(e))




# Rota para testar conexões com RabbitMQ e MySQL
@app.route('/testar-conexoes', methods=['GET'])
def testar_conexoes():
    try:
        channel = conectar_rabbitmq()
        if channel:
            logging.info("Conexão ao RabbitMQ bem-sucedida")
            channel.close()
        else:
            logging.error("Erro na conexão ao RabbitMQ")

        connection = conectar_mysql()
        if connection:
            logging.info("Conexão ao MySQL bem-sucedida")
            connection.close()
        else:
            logging.error("Erro na conexão ao MySQL")

        return jsonify({'message': 'Teste de conexões realizado com sucesso!'}), 200

    except Exception as e:
        logging.error("Erro ao testar conexões: %s", str(e))
        return jsonify({'error': str(e)}), 500

# Handle OPTIONS requests globally for CORS
@app.route('/', methods=['OPTIONS'])
def options():
    response = make_response()
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'POST')
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0')
