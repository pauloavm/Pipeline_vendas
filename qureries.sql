SHOW DATABASES;

USE xingcel;

SHOW TABLES;

SHOW COLUMNS FROM Vendas;

SELECT * FROM Produtos LIMIT 10;

SELECT * FROM Vendas LIMIT 10;

SELECT * FROM Clientes LIMIT 10;

SELECT COUNT(*) FROM Vendas;

SELECT COUNT(*) FROM Produtos;

SELECT MIN(data_venda), MAX(data_venda) FROM Vendas;

SELECT id_produto, SUM(quantidade) AS total_venda
FROM Vendas
GROUP BY id_produto
ORDER BY total_venda DESC
LIMIT 10;