import json
import boto3
import uuid
from botocore.exceptions import ClientError
import os

# Recursos AWS
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

table = dynamodb.Table('contatos')

# 🔥 Coloque aqui o ARN do seu SNS
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:189100467174:mova_contatos'

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        nome = body.get('nome')
        email = body.get('email')
        telefone = body.get('telefone')
        empresa = body.get('empresa')
        cargo = body.get('cargo')
        comentarios = body.get('comentarios')
        plano = body.get('plano')

        if not nome or not email or not plano or not telefone:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'erro': 'Campos obrigatórios: nome, email, plano, telefone'
                })
            }

        # ✅ Gera UUID
        id_contato = str(uuid.uuid4())

        item = {
            'id_contato': id_contato,
            'email': email,
            'nome': nome,
            'telefone': telefone,
            'empresa': empresa,
            'cargo': cargo,
            'comentarios': comentarios,
            'plano': plano
        }

        # 🔹 Salva no DynamoDB
        table.put_item(Item=item)

        # 🔹 Publica no SNS
        mensagem = f"Novo cliente acessou a plataforma do Mova. Entre em contato no numero {telefone} ou e-mail {email}"

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=mensagem,
            Subject=f'Novo contato cadastrado - {nome}'
        )

        return {
            'statusCode': 201,
            'body': json.dumps({
                'mensagem': 'Registro salvo e notificação enviada',
                'id_contato': id_contato
            })
        }

    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'erro': e.response['Error']['Message']
            })
        }