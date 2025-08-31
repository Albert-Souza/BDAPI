import psycopg2
from psycopg2 import errors
from psycopg2.extras import RealDictCursor
import datetime

class PostgresCRUD:
    def __init__(self, dbname="db", user="db", password="db", host="localhost", port="5432"):
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

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
        try:
            self.cur.execute("""
                INSERT INTO BancoDeDados.Cliente 
                (cpf, nome_primeiro, nome_sobrenome, data_nascimento, endereco, telefone, email)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (cpf, nome_primeiro, nome_sobrenome, data_nascimento, endereco, telefone, email))
            self.conn.commit()
            self._log("ok", f"Cliente `{cpf}` inserido com sucesso!")
        except errors.UniqueViolation:
            self.conn.rollback()
            self._log("warn", f"Cliente `{cpf}` já existe, não foi inserido.")
        except Exception as e:
            self.conn.rollback()
            self._log("err", f"Erro ao inserir cliente: {e}")

    # ---------------- CONTA ----------------
    def insert_conta(self, numero_conta, cpf_cliente, tipo_conta, saldo):
        try:
            self.cur.execute("""
                INSERT INTO BancoDeDados.Conta
                (numero_conta, cpf_cliente, tipo_conta, saldo, data_abertura)
                VALUES (%s,%s,%s,%s,%s)
            """, (numero_conta, cpf_cliente, tipo_conta, saldo, datetime.date.today()))
            self.conn.commit()
            self._log("ok", f"Conta `{numero_conta}` criada com sucesso!")
        except errors.UniqueViolation:
            self.conn.rollback()
            self._log("warn", f"Conta `{numero_conta}` já existe, não foi criada.")
        except errors.ForeignKeyViolation:
            self.conn.rollback()
            self._log("warn", f"CPF `{cpf_cliente}` não existe, não foi possível criar a conta.")
        except Exception as e:
            self.conn.rollback()
            self._log("err", f"Erro ao inserir conta: {e}")

    # ---------------- TRANSACAO ----------------
    def insert_transacao(self, id_transacao, conta_origem, conta_destino, valor, tipo):
        try:
            self.cur.execute("""
                INSERT INTO BancoDeDados.Transacao
                (id_transacao, conta_origem, conta_destino, valor, data_hora, tipo_transacao)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (id_transacao, conta_origem, conta_destino, valor, datetime.datetime.now(), tipo))
            self.conn.commit()
            self._log("ok", f"Transação `{id_transacao}` registrada com sucesso!")
        except errors.UniqueViolation:
            self.conn.rollback()
            self._log("warn", f"Transação `{id_transacao}` já existe, não foi inserida.")
        except errors.ForeignKeyViolation:
            self.conn.rollback()
            self._log("warn", f"Conta origem/destino inválida, não foi possível registrar transação.")
        except Exception as e:
            self.conn.rollback()
            self._log("err", f"Erro ao inserir transação: {e}")

    # ---------------- GET & DELETE ----------------
    def get_cliente(self, cpf):
        self.cur.execute("SELECT * FROM BancoDeDados.Cliente WHERE cpf=%s", (cpf,))
        return self.cur.fetchone()

    def update_cliente(self, cpf, novo_endereco):
        self.cur.execute("UPDATE BancoDeDados.Cliente SET endereco=%s WHERE cpf=%s", (novo_endereco, cpf))
        self.conn.commit()
        self._log("ok", f"Endereço do cliente `{cpf}` atualizado para `{novo_endereco}`.")

    def delete_cliente(self, cpf):
        self.cur.execute("DELETE FROM BancoDeDados.Cliente WHERE cpf=%s", (cpf,))
        self.conn.commit()
        self._log("ok", f"Cliente `{cpf}` deletado.")

    def get_conta(self, numero_conta):
        self.cur.execute("SELECT * FROM BancoDeDados.Conta WHERE numero_conta=%s", (numero_conta,))
        return self.cur.fetchone()
    
    def delete_conta(self, numero_conta):
        self.cur.execute("DELETE FROM BancoDeDados.Conta WHERE numero_conta=%s", (numero_conta,))
        self.conn.commit()
        self._log("ok", f"Conta `{numero_conta}` deletada.")

    def get_transacao(self, id_transacao):
        self.cur.execute("SELECT * FROM BancoDeDados.Transacao WHERE id_transacao=%s", (id_transacao,))
        return self.cur.fetchone()
    
    def delete_transacoes_por_conta(self, numero_conta):
        self.cur.execute("""
            DELETE FROM BancoDeDados.Transacao
            WHERE conta_origem=%s OR conta_destino=%s
        """, (numero_conta, numero_conta))
        self.conn.commit()
        self._log("ok", f"Todas as transações da conta `{numero_conta}` foram deletadas.")

    def show_cliente(self, cpf):
        cliente = self.get_cliente(cpf)
        if cliente:
            self._log("info", f"Cliente `{cliente['cpf']}` | {cliente['nome_primeiro']} {cliente['nome_sobrenome']} | Endereço: {cliente['endereco']} | Tel: {cliente['telefone']} | Email: {', '.join(cliente['email'])}")
        else:
            self._log("warn", f"Nenhum cliente encontrado com CPF `{cpf}`.")

    def show_conta(self, numero_conta):
        conta = self.get_conta(numero_conta)
        if conta:
            self._log("info", f"Conta `{conta['numero_conta']}` | Cliente `{conta['cpf_cliente']}` | Tipo: {conta['tipo_conta']} | Saldo: R$ {conta['saldo']} | Abertura: {conta['data_abertura']}")
        else:
            self._log("warn", f"Nenhuma conta encontrada com número `{numero_conta}`.")

    def show_transacao(self, id_transacao):
        tx = self.get_transacao(id_transacao)
        if tx:
            self._log("info", f"Transação `{tx['id_transacao']}` | {tx['tipo_transacao']} | Origem: {tx['conta_origem']} → Destino: {tx['conta_destino']} | Valor: R$ {tx['valor']} | Data: {tx['data_hora']}")
        else:
            self._log("warn", f"Nenhuma transação encontrada com ID `{id_transacao}`.")


    def close(self):
        self.cur.close()
        self.conn.close()
        self._log("info", "Conexão com o banco encerrada.")
