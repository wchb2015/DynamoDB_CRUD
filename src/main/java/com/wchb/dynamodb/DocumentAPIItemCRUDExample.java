package com.wchb.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DocumentAPIItemCRUDExample {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static String tableName = "RegisteredMobileEndpoints";

    public void createItems()
    {

        final ImmutableMap.Builder<String, AttributeValue> itemBuilder =
                new ImmutableMap.Builder<>();
        itemBuilder.put("TIMESTAMP_ATTR",
                new AttributeValue().withN(String.valueOf(System.currentTimeMillis())))
                .put("Update_TIMESTAMP_ATTR",
                        new AttributeValue().withN(String.valueOf(System.currentTimeMillis())))
                .put("customerId", new AttributeValue().withS("1234"))
                .put("endpointId", new AttributeValue().withS("abc"));

        final PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName);
        putItemRequest.withItem(itemBuilder.build());

        client.putItem(putItemRequest);
    }

    public void retrieveItem()
    {
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        Map<String, AttributeValue> lastKey = null;
        QueryResult queryResult;

        QueryRequest queryRequest = new QueryRequest(tableName);
        queryRequest.setConsistentRead(true);

        queryRequest.withKeyConditions(ImmutableMap.of("customerId", equalTo("1234")));

        do
        {
            queryRequest.setExclusiveStartKey(lastKey);
            try
            {
                queryResult = client.query(queryRequest);
                items.addAll(queryResult.getItems());
                lastKey = queryResult.getLastEvaluatedKey();
            } catch (AmazonClientException ex)
            {
            } finally
            {
            }

        } while (lastKey != null);

        System.out.println(items);
    }

    public void deleteItem()
    {
        List<WriteRequest> deleteRequestList = new ArrayList<>();

        final DeleteRequest deleteRequest = new DeleteRequest().withKey(
                buildKeyConditions(
                        "1234",
                        "abc"));

        deleteRequestList.add(new WriteRequest(deleteRequest));

        Map<String, List<WriteRequest>> deleteItems =
                ImmutableMap.of(tableName, deleteRequestList);

        final BatchWriteItemRequest batchDeleteRequest = new BatchWriteItemRequest();
        BatchWriteItemResult batchDeleteResult;

        do
        {
            batchDeleteRequest.withRequestItems(deleteItems);
            try
            {
                batchDeleteResult = client.batchWriteItem(batchDeleteRequest);
                deleteItems = batchDeleteResult.getUnprocessedItems();
            } catch (Exception ex)
            {
            } finally
            {
            }
        } while (!deleteItems.isEmpty());

    }

    private Map<String, AttributeValue> buildKeyConditions(
            final String customerId,
            final String endpointId)
    {
        return ImmutableMap.of(
                "customerId", new AttributeValue(customerId),
                "endpointId", new AttributeValue(endpointId));
    }

    private Condition equalTo(final String attributeValue)
    {
        return new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(attributeValue));
    }
}
