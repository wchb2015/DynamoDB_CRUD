package com.wchb.dynamodb;

import org.junit.Test;

public class DocumentAPIItemCRUDExampleTest {

    private DocumentAPIItemCRUDExample example = new DocumentAPIItemCRUDExample();

    @Test
    public void createItems()
    {
        example.createItems();
    }

    @Test
    public void retrieveItem()
    {
        example.retrieveItem();
    }

    @Test
    public void deleteItem()
    {
        example.deleteItem();
    }
}