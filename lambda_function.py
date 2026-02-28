import json
import boto3
import uuid
import os
from botocore.exceptions import ClientError

HTTP_CREATED = 201
HTTP_BAD_REQUEST = 400
HTTP_INTERNAL_SERVER_ERROR = 500
TABLE_NAME = "contatos"
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        print("contatos")
        validar_configuracao()

        body = obter_body(event)

        nome = body.get("nome")
        email = body.get("email")
        telefone = body.get("telefone")
        empresa = body.get("empresa")
        cargo = body.get("cargo")
        comentarios = body.get("comentarios")
        plano = body.get("plano")

        validar_campos(nome, email, telefone, plano)

        id_contato = gerar_uuid()

        item = {
            "id_contato": id_contato,
            "email": email,
            "nome": nome,
            "telefone": telefone,
            "empresa": empresa,
            "cargo": cargo,
            "comentarios": comentarios,
            "plano": plano
        }

        salvar_no_dynamodb(item)
        publicar_no_sns(nome, email, telefone)

        return resposta(
            HTTP_CREATED,
            {
                "mensagem": "Registro salvo e notificação enviada",
                "id_contato": id_contato
            }
        )

    except ValueError as erro_validacao:
        return resposta(HTTP_BAD_REQUEST, {"erro": str(erro_validacao)})

    except ClientError as erro_aws:
        return resposta(
            HTTP_INTERNAL_SERVER_ERROR,
            {"erro": erro_aws.response["Error"]["Message"]}
        )

    except Exception as erro_generico:
        return resposta(
            HTTP_INTERNAL_SERVER_ERROR,
            {"erro": str(erro_generico)}
        )

def validar_configuracao():
    if not SNS_TOPIC_ARN:
        raise Exception("Variável de ambiente SNS_TOPIC_ARN não configurada")


def obter_body(event):
    return json.loads(event.get("body", "{}"))

def validar_campos(nome, email, telefone, plano):
    if not nome or not email or not telefone or not plano:
        raise ValueError(
            "Campos obrigatórios: nome, email, telefone, plano"
        )

def gerar_uuid():
    return str(uuid.uuid4())

def salvar_no_dynamodb(item):
    table.put_item(Item=item)

def publicar_no_sns(nome, email, telefone):
    mensagem = (
        f"Novo cliente acessou a plataforma do Mova.\n"
        f"Telefone: {telefone}\n"
        f"E-mail: {email}"
    )

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=mensagem,
        Subject=f"Novo contato cadastrado - {nome}"
    )

def resposta(status_code, corpo):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(corpo)
    }