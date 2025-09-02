# crud_mongo.py
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import datetime
from typing import Optional, Union, List

class MongoCRUD:
    def __init__(self, uri="", db_name="Banco"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        # Collections
        self.clientes = self.db["Cliente"]
        self.contas = self.db["Conta"]
        self.transacoes = self.db["Transacao"]
        # Indexes / Unique constraints similar to RDBMS unique keys
        self.clientes.create_index([("cpf", ASCENDING)], unique=True)
        self.contas.create_index([("numero_conta", ASCENDING)], unique=True)
        self.transacoes.create_index([("id_transacao", ASCENDING)], unique=True)

    # ---------------- PRINT HELPER ----------------
    def _log(self, tipo, mensagem):
        icons = {
            "ok": "✅",
            "warn": "⚠️",
            "err": "❌",
            "info": "ℹ️"
        }
        print(f"{icons.get(tipo, 'ℹ️')} {mensagem}")

    # ---------------- CLIENTE ----------------
    def insert_cliente(self, cpf, nome_primeiro, nome_sobrenome, data_nascimento, endereco, telefone, email):

        # normalize email to list
        if isinstance(email, str):
            emails = [email]
        else:
            emails = list(email)

        

        doc = {
            "cpf": cpf,
            "nome": {
            "primeiro": nome_primeiro,
            "sobrenome": nome_sobrenome
            },
            "data_nascimento": data_nascimento,
            "endereco": endereco,
            "telefone": telefone,
            "email": emails,
        }
        try:
            self.clientes.insert_one(doc)
            self._log("ok", f"Cliente `{cpf}` inserido com sucesso!")
        except DuplicateKeyError:
            self._log("warn", f"Cliente `{cpf}` já existe, não foi inserido.")
        except Exception as e:
            self._log("err", f"Erro ao inserir cliente: {e}")

    # ---------------- CONTA ----------------
    def insert_conta(self, numero_conta: str, cpf_cliente: str, tipo_conta: str, saldo: float):
        # checar se cliente existe (simulando foreign key)
        cliente = self.clientes.find_one({"cpf": cpf_cliente})
        if not cliente:
            self._log("warn", f"CPF `{cpf_cliente}` não existe, não foi possível criar a conta.")
            return

        doc = {
            "numero_conta": numero_conta,
            "cpf_cliente": cpf_cliente,
            "tipo_conta": tipo_conta,
            "saldo": float(saldo)
        }
        try:
            self.contas.insert_one(doc)
            self._log("ok", f"Conta `{numero_conta}` criada com sucesso!")
        except DuplicateKeyError:
            self._log("warn", f"Conta `{numero_conta}` já existe, não foi criada.")
        except Exception as e:
            self._log("err", f"Erro ao inserir conta: {e}")

    # ---------------- TRANSACAO ----------------
    def insert_transacao(self, id_transacao: str, conta_origem: str, conta_destino: str,
                         valor: float, tipo: str):
        # checar se contas existem (simulando foreign keys)
        origem = self.contas.find_one({"numero_conta": conta_origem})
        destino = self.contas.find_one({"numero_conta": conta_destino})
        if not origem or not destino:
            self._log("warn", f"Conta origem/destino inválida, não foi possível registrar transação.")
            return

        doc = {
            "id_transacao": id_transacao,
            "conta_origem": conta_origem,
            "conta_destino": conta_destino,
            "valor": float(valor),
            "tipo_transacao": tipo
        }
        try:
            self.transacoes.insert_one(doc)
            self._log("ok", f"Transação `{id_transacao}` registrada com sucesso!")
        except DuplicateKeyError:
            self._log("warn", f"Transação `{id_transacao}` já existe, não foi inserida.")
        except Exception as e:
            self._log("err", f"Erro ao inserir transação: {e}")

    # ---------------- GET & DELETE ----------------
    def get_cliente(self, cpf: str) -> Optional[dict]:
        return self.clientes.find_one({"cpf": cpf})

    def update_cliente(self, cpf: str, novo_endereco: str):
        res = self.clientes.update_one({"cpf": cpf}, {"$set": {"endereco": novo_endereco}})
        if res.matched_count:
            self._log("ok", f"Endereço do cliente `{cpf}` atualizado para `{novo_endereco}`.")
        else:
            self._log("warn", f"Nenhum cliente encontrado com CPF `{cpf}` para atualizar.")

    def delete_cliente(self, cpf: str):
        res = self.clientes.delete_one({"cpf": cpf})
        if res.deleted_count:
            self._log("ok", f"Cliente `{cpf}` deletado.")
        else:
            self._log("warn", f"Nenhum cliente com CPF `{cpf}` encontrado para deletar.")

    def get_conta(self, numero_conta: str) -> Optional[dict]:
        return self.contas.find_one({"numero_conta": numero_conta})

    def delete_conta(self, numero_conta: str):
        res = self.contas.delete_one({"numero_conta": numero_conta})
        if res.deleted_count:
            self._log("ok", f"Conta `{numero_conta}` deletada.")
        else:
            self._log("warn", f"Nenhuma conta encontrada com número `{numero_conta}` para deletar.")

    def get_transacao(self, id_transacao: str) -> Optional[dict]:
        return self.transacoes.find_one({"id_transacao": id_transacao})

    def delete_transacoes_por_conta(self, numero_conta: str):
        res = self.transacoes.delete_many({"$or": [{"conta_origem": numero_conta}, {"conta_destino": numero_conta}]})
        self._log("ok", f"{res.deleted_count} transação(ões) da conta `{numero_conta}` foram deletadas.")

    # ---------------- SHOW HELPERS ----------------
    def show_cliente(self, cpf: str):
        cliente = self.get_cliente(cpf)
        if cliente:
            emails = cliente.get("email", [])
            if isinstance(emails, list):
                emails_str = ", ".join(emails)
            else:
                emails_str = str(emails)
            self._log("info", f"Cliente `{cliente.get('cpf')}` | {cliente.get('nome_primeiro')} {cliente.get('nome_sobrenome')} | Endereço: {cliente.get('endereco')} | Tel: {cliente.get('telefone')} | Email: {emails_str}")
        else:
            self._log("warn", f"Nenhum cliente encontrado com CPF `{cpf}`.")

    def show_conta(self, numero_conta: str):
        conta = self.get_conta(numero_conta)
        if conta:
            self._log("info", f"Conta `{conta.get('numero_conta')}` | Cliente `{conta.get('cpf_cliente')}` | Tipo: {conta.get('tipo_conta')} | Saldo: R$ {conta.get('saldo')} | Abertura: {conta.get('data_abertura')}")
        else:
            self._log("warn", f"Nenhuma conta encontrada com número `{numero_conta}`.")

    def show_transacao(self, id_transacao: str):
        tx = self.get_transacao(id_transacao)
        if tx:
            self._log("info", f"Transação `{tx.get('id_transacao')}` | {tx.get('tipo_transacao')} | Origem: {tx.get('conta_origem')} → Destino: {tx.get('conta_destino')} | Valor: R$ {tx.get('valor')} | Data: {tx.get('data_hora')}")
        else:
            self._log("warn", f"Nenhuma transação encontrada com ID `{id_transacao}`.")

    def close(self):
        self.client.close()
        self._log("info", "Conexão com o banco MongoDB encerrada.")


# Exemplo de uso rápido
if __name__ == "__main__":
    db = MongoCRUD(uri="mongodb://localhost:27017", db_name="BancoDeDados")

    # inserir cliente
    db.insert_cliente(
        cpf="12345678900",
        nome_primeiro="João",
        nome_sobrenome="Silva",
        data_nascimento="1985-04-12",
        endereco="Rua A, 123",
        telefone="+55 11 91234-5678",
        email=["joao@example.com"]
    )

    # criar conta
    db.insert_conta(
        numero_conta="0001-01",
        cpf_cliente="12345678900",
        tipo_conta="corrente",
        saldo=1500.0
    )

    # registrar transação
    # Para o exemplo vamos criar uma segunda conta para destino
    db.insert_conta("0001-02", "12345678900", "poupanca", 500.0)
    db.insert_transacao("tx-0001", "0001-01", "0001-02", 200.0, "transferencia")

    # mostrar registros
    db.show_cliente("12345678900")
    db.show_conta("0001-01")
    db.show_transacao("tx-0001")

    # limpeza (opcional)
    db.delete_transacoes_por_conta("0001-01")
    db.delete_conta("0001-01")
    db.delete_conta("0001-02")
    db.delete_cliente("12345678900")

    db.close()
