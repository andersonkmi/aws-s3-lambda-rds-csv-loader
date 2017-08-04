package org.sharpsw.data;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class ItemDataLoader implements DataLoader {
    private static final String INSERT_SCRIPT = "INSERT INTO dbo.ITEM(ITEM_SK, ITEM_COD, ITEM_DESC, FABRICANTE_SK, ITEM_CATEGORIA, ITEM_SUBCATEGORIA, ITEM_UNIDADE_VENDA_TIPO, ITEM_UNIDADE_VENDA_QTD, ITEM_MEDIDA_TIPO, ITEM_MEDIDA_QTD, ITEM_EMBALAGEM_TIPO, ITEM_MARCA, ITEM_VERSAO, ITEM_IMPORTACAO_IND, ITEM_CODIFICACAO_TIPO, INICIO_VIGENCIA_DT, FIM_VIGENCIA_DT, ULTIMA_ATUALIZACAO_DT, EXECUCAO_NUM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SELECT_SCRIPT = "SELECT COUNT(1) FROM dbo.ITEM WHERE ITEM_SK = ?";
    private static final String UPDATE_SCRIPT = "UPDATE dbo.ITEM SET ITEM_COD = ?, ITEM_DESC = ?, FABRICANTE_SK = ?, ITEM_CATEGORIA = ?, ITEM_SUBCATEGORIA = ?, ITEM_UNIDADE_VENDA_TIPO = ?, ITEM_UNIDADE_VENDA_QTD = ?, ITEM_MEDIDA_TIPO = ?, ITEM_MEDIDA_QTD = ?, ITEM_EMBALAGEM_TIPO = ?, ITEM_MARCA = ?, ITEM_VERSAO = ?, ITEM_IMPORTACAO_IND = ?, ITEM_CODIFICACAO_TIPO = ?, INICIO_VIGENCIA_DT = ?, FIM_VIGENCIA_DT = ?, ULTIMA_ATUALIZACAO_DT = ?, EXECUCAO_NUM = ? WHERE ITEM_SK = ?";

    public void load(Connection connection, String line) throws SQLException {
        List<String> tokens = getTokens(line);
        String sk = tokens.get(0);

        if(isInsert(sk, connection)) {
            insert(tokens, connection);
        } else {
            update(tokens, connection);
        }
    }

    private boolean isInsert(String sk, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(SELECT_SCRIPT);
        statement.setInt(1, Integer.getInteger(sk));
        ResultSet rs = statement.executeQuery();
        int count = 0;
        if(rs.next()) {
            count = rs.getInt(0);
        }
        rs.close();

        return count == 0;
    }


    private List<String> getTokens(String line) {
        List<String> tokens = new LinkedList<String>();
        StringTokenizer tokenizer = new StringTokenizer(line, ";");
        while(tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }

        return tokens;
    }

    private void insert(List<String> tokens, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(INSERT_SCRIPT);

        statement.setInt(1, Integer.getInteger(tokens.get(0)));
        statement.setString(2, tokens.get(1));
        statement.setString(3, tokens.get(2));
        statement.setInt(4, Integer.getInteger(tokens.get(3)));
        statement.setString(5, tokens.get(4));
        statement.setString(6, tokens.get(5));
        statement.setString(7, tokens.get(6));
        statement.setInt(8, Integer.getInteger(tokens.get(7)));
        statement.setString(9, tokens.get(8));
        statement.setBigDecimal(10, new BigDecimal(tokens.get(9)));
        statement.setString(11, tokens.get(10));
        statement.setString(12, tokens.get(11));
        statement.setString(13, tokens.get(12));
        statement.setString(14, tokens.get(13));
        statement.setString(15, tokens.get(14));
        statement.setDate(16, Date.valueOf(tokens.get(15)));
        statement.setDate(17, Date.valueOf(tokens.get(16)));
        statement.setDate(18, Date.valueOf(tokens.get(17)));
        statement.setInt(19, Integer.getInteger(tokens.get(18)));

        statement.executeQuery();
    }

    private void update(List<String> tokens, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(UPDATE_SCRIPT);
        statement.setString(1, tokens.get(1));
        statement.setString(2, tokens.get(2));
        statement.setInt(3, Integer.getInteger(tokens.get(3)));
        statement.setString(4, tokens.get(4));
        statement.setString(5, tokens.get(5));
        statement.setString(6, tokens.get(6));
        statement.setInt(7, Integer.getInteger(tokens.get(7)));
        statement.setString(8, tokens.get(8));
        statement.setBigDecimal(9, new BigDecimal(tokens.get(9)));
        statement.setString(10, tokens.get(10));
        statement.setString(11, tokens.get(11));
        statement.setString(12, tokens.get(12));
        statement.setString(13, tokens.get(13));
        statement.setString(14, tokens.get(14));
        statement.setDate(15, Date.valueOf(tokens.get(15)));
        statement.setDate(16, Date.valueOf(tokens.get(16)));
        statement.setDate(17, Date.valueOf(tokens.get(17)));
        statement.setInt(18, Integer.getInteger(tokens.get(18)));
        statement.setInt(19, Integer.getInteger(tokens.get(0)));

        statement.executeQuery();
    }
}
