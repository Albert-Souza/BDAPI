from Crud_MongoDB import MongoCRUD

if __name__ == "__main__":
    pg = MongoCRUD()

    # CREATE - Cliente 1
    pg.insert_cliente("12345678900", "João", "Silva", "1990-01-01", "Rua A", "11999999999", ["joao@email"])
    pg.insert_conta("000000000001", "12345678900", "Corrente", 1000)

    # CREATE - Cliente 2
    pg.insert_cliente("98765432100", "Maria", "Oliveira", "1992-05-10", "Rua B", "11888888888", ["maria@email"])
    pg.insert_conta("000000000002", "98765432100", "Poupança", 500)

    # TRANSACAO (João → Maria)
    pg.insert_transacao(1, "000000000001", "000000000002", 200, "PIX")

    # READ
    pg.show_conta("000000000001")
    pg.show_conta("000000000002")
    pg.show_transacao(1)
    pg.show_cliente("12345678900")
    pg.show_cliente("98765432100")

    # UPDATE
    pg.update_cliente("12345678900", "Rua Nova A")
    pg.show_cliente("12345678900")

    # DELETE
    pg.delete_transacoes_por_conta("000000000001")
    pg.delete_transacoes_por_conta("000000000002")

    pg.delete_conta("000000000001")
    pg.delete_conta("000000000002")

    pg.delete_cliente("12345678900")
    pg.delete_cliente("98765432100")

    pg.close()
