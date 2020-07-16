package controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.ServiceException;
import connections.ConnectionHandler;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "API")
@RestController
@RequestMapping("cgu/api/v1/hbase")
public class HBaseController {

    @Autowired
    ConnectionHandler connectionHandler;

    @Value("${cluster.hbase.conf}")
    private String hbaseSite;

    @Value("${cluster.core.conf}")
    private String coreSite;


    @PreAuthorize("#oauth2.hasScope('api.cgu.customers')")
    @GetMapping(value = "/1000/customers")
    @ApiOperation(value = "Select all from customers where given customer from hbase", notes = "Return customer information given its ID", response = String.class)
    public HashMap<String, String> selectCustomerFromHbase_1000(@RequestParam(name = "c_customer_sk", required = true, defaultValue = "1") String customerID) throws IOException, ServiceException {

        Connection connection = connectionHandler.getHBaseConnection();

        Table table = connection.getTable(TableName.valueOf("hbase_customer_1000"));
        Get get = new Get(Bytes.toBytes(customerID));

        return hbaseGetToHashMap(table, get, "hbase_customer_1000");
    }

    @PreAuthorize("#oauth2.hasScope('api.cgu.customers')")
    @GetMapping(value = "/customers")
    @ApiOperation(value = "Select all from customers where given customer from hbase", notes = "Return customer information given its ID", response = String.class)
    public HashMap<String, String> selectCustomerFromHbase(@RequestParam(name = "c_customer_sk", required = true, defaultValue = "1") String customerID) throws IOException, ServiceException {

        Connection connection = connectionHandler.getHBaseConnection();

        Table table = connection.getTable(TableName.valueOf("hbase_customer"));
        Get get = new Get(Bytes.toBytes(customerID));

        return hbaseGetToHashMap(table, get, "hbCustomer");
    }

    @PreAuthorize("#oauth2.hasScope('api.cgu.customers')")
    @GetMapping(value = "/customers-details")
    @ApiOperation(value = "Select customers details where given customer from hbase", notes = "Return customer information & other details given its ID", response = String.class)
    public HashMap<String, String> selectCustomerDetailsFromHbase(@RequestParam(name = "c_customer_sk", required = true, defaultValue = "1") String customerID) throws IOException, ServiceException {

        Connection connection = connectionHandler.getHBaseConnection();

        Table table = connection.getTable(TableName.valueOf("hbase_joins_query"));
        Get get = new Get(Bytes.toBytes(customerID));

        return hbaseGetToHashMap(table, get, "f");
    }

    @PreAuthorize("#oauth2.hasScope('api.cgu.customers')")
    @GetMapping(value = "/worse")
    @ApiOperation(value = "Executes worst case scenario query on HBase", notes = "Returns 1 customers details", response = String.class)
    public HashMap<String, String> worstCaseFromHbase(@RequestParam(name = "item_sk", required = true, defaultValue = "17902") String itemSk,
                                                      @RequestParam(name = "ticket_number", required = true, defaultValue = "20040") String ticketNb) throws IOException, ServiceException {

        Connection connection = connectionHandler.getHBaseConnection();

        Table table = connection.getTable(TableName.valueOf("worst_case"));
        Get get = new Get(Bytes.toBytes(itemSk + "_" + ticketNb));

        return hbaseGetToHashMap(table, get, "d");
    }

    /**
     *
     * @param table
     * @param get
     * @param columnFamily
     * @return
     * @throws IOException
     */
    private HashMap<String, String> hbaseGetToHashMap(Table table, Get get, String columnFamily) throws IOException {

        Result result = table.get(get);
        HashMap<String, String> resultMap = new HashMap<>();
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(columnFamily));
        Set<byte[]> keys = result.getFamilyMap(Bytes.toBytes(columnFamily)).keySet();
        keys.stream().forEach(k -> {
            resultMap.put(Bytes.toString(k), Bytes.toString(map.get(k)));
        });
        return resultMap;

    }

}