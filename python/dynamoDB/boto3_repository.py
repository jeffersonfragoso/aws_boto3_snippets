import boto3
import logging

from boto3.dynamodb.conditions import And, Attr
from botocore.exceptions import ClientError
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


class DynamoDbRepository:

    def __init__(self, table_name: str):
        self.table = boto3.resource("dynamodb").Table(table_name)

    def insert_many(self, data: List[Dict]):
        """
        Table.batch_writer cria uma lista de requests.
        Ao sair do context manager(with), Table.batch_writer começa a enviar
        lotes de solicitações para o Amazon DynamoDB e automaticamente lida com chunking, buffering, and retrying.
        Suporta até 25 itens de 400 KB ou no máximo 16 MB por requests, o que ocorrer primeiro.

        :param data (List[Dict]): Lista de items para inserir na tabela.
        """

        try:
            with self.table.batch_writer() as writer:
                for item in data:
                    writer.put_item(Item=item)
        except ClientError as err:
            msg = f"Não foi possível inserir os dados na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']

            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise

    def insert(self, item: Dict[str]):
        """
        Adiciona um item.

        :param item: Item a ser inserido.
        """
        try:
            self.table.put_item(Item=item)
        except ClientError as err:
            msg = f"Não foi possível inserir o item ({item}) na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']

            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise

    def find_by_id(self, item_id: str):
        """
        Pesquisa um item pelo id.

        :param item_id: Id do item a ser pesquisado.
        """
        try:
            response = self.table.get_item(Key={'id': item_id})
        except ClientError as err:
            msg = f"Não foi possível realizar a pesquisa na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']

            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise
        else:
            item = response.get('Item', None)
            if item:
                return item
            else:
                msg = f"Item ({item_id}) não encontrado na tabela ({self.table.name})."
                logger.error(f"{msg}")
                raise

    def update(
        self,
        item_id: str,
        update_expression: str,
        expression_attribute_values: Dict[str],
        condition_expression: Any,
        expression_attribute_names: Dict[str] = {}
    ):
        """
        Atualiza um item.

        :param item_id (str): Id do item a ser atualizado.
        :param update_expression (str): Informa quais campos serão atualizados
        :param expression_attribute_values (dict): Informa os novos valores para cada campo.
        :param condition_expression: Condições construídas com boto3.dynamodb.conditions
        :param expression_attribute_names: Opcional. Caso algum campo corresponda a uma palavra reservada.

            Ex:
                'status' é uma palavra reservada e não pode ser passado na expressão 'set',
                então pode ser substituido por um curinga como '#S'

                UpdateExpression="set #S=:s key1=:k1 key2=:k2"
                expression_attribute_names={"#S": "status"}
                expression_attribute_values={":s": "Novo status", "k1": "Novo key1", "k2": "Novo key2"}

        """
        try:
            condition_required = Attr("id").exists()

            if not condition_expression:
                condition_expression = condition_required
            else:
               condition_expression = And(condition_required, condition_expression)

            response = self.table.update_item(
                Key={'id': item_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ConditionExpression=condition_expression,
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as err:
            msg = f"Não foi possível atualizar o item ({item_id}) na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']

            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise
        else:
            return response['Attributes']

    def find_all(self, start_key=None, limit: int = 100):
        """
        Scans for items.

        :param start_key: Posição em que o scan irá iniciar.
        :param limit (int): Quantidade de registros retornardos.
        """
        items = []

        scan_args = {
            "Limit": limit,
        }

        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_args['ExclusiveStartKey'] = start_key
                response = self.table.scan(**scan_args)
                items.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except ClientError as err:
            msg = f"Não foi possível realizar o Scan na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']

            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise

        return items

    def delete(self, item_id):
        """
        Deleta um item.

        :param item_id (str): Id do item a ser atualizado.
        """
        try:
            self.table.delete_item(Key={'id': item_id})
        except ClientError as err:
            msg = f"Não foi possível deleter o item ({item_id}) na tabela ({self.table.name})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']
            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise

    def search(self, field: str, value: str):
        """
        Pesquisa pelos itens que contém valor correspodente no campo especificado.

        :param field (str): Campo da tabela.
        :param value (str): Valor a ser pesquisado no campo.
        """
        try:
            response = self.table.query(FilterExpression=Attr(field).contains(value))
        except ClientError as err:
            msg = f"Não foi possível pesquisar itens no campo ({field}) que contém ({value})."
            error_code = err.response['Error']['Code']
            error_msg = err.response['Error']['Message']
            logger.error(f"{msg}\n{error_code}:{error_msg}")
            raise
        else:
            return response['Items']
