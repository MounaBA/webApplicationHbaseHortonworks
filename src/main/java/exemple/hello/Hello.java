package exemple.hello;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import java.time.Instant;
import java.util.UUID;

@Path("/h")
public class Hello {
	static Configuration conf = HBaseConfiguration.create();
	static {
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	}

	@Path("/find")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String find() {
		StringBuilder sb = new StringBuilder();

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = new HTable(conf, "TableActivity");
			ResultScanner rs = table.getScanner(Bytes.toBytes("name"));

			for (Result result : rs) {
				for (Cell cell : result.listCells()) {
					sb.append(new String(CellUtil.cloneFamily(cell)) + "=" + new String(CellUtil.cloneValue(cell)));
					sb.append(";");
				}
				sb.append("\n");
			}
			table.close();

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return sb.toString();
	}

	@Path("/saveActivity")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String whatIsHeDoing(@QueryParam("name") String name, @QueryParam("activity") String activity) {
		if ((name == null) || (activity == null)) {
			return "missing name or activity";
		}
		try {
			String tableName = "TableActivity";
			// create an admin object using the config
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName)) {
				// create the table...
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
				// ... with two column families
				tableDescriptor.addFamily(new HColumnDescriptor("name"));
				tableDescriptor.addFamily(new HColumnDescriptor("activity"));
				tableDescriptor.addFamily(new HColumnDescriptor("date"));
				admin.createTable(tableDescriptor);
			}
			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = new HTable(conf, "TableActivity");

			// Add each person to the table
			// Use the `name` column family for the name
			// Use the `contactinfo` column family for the email

			Put person = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
			person.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
			person.add(Bytes.toBytes("activity"), Bytes.toBytes("activity"), Bytes.toBytes(activity));
			person.add(Bytes.toBytes("date"), Bytes.toBytes("date"), Bytes.toBytes(Instant.now().toString()));
			table.put(person);

			// flush commits and close the table
			table.flushCommits();
			table.close();
			System.out.println("activity saved ");

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return "Bonjour, tout le monde!";
	}

	@Path("/savePlace")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String whereIsHe(@QueryParam("name") String name, @QueryParam("place") String place) {
		if ((name == null) || (place == null)) {
			return "missing name or place";
		}
		try {
			String tableName = "TablePlace";
			// create an admin object using the config
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName)) {
				// create the table...
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
				// ... with two column families
				tableDescriptor.addFamily(new HColumnDescriptor("name"));
				tableDescriptor.addFamily(new HColumnDescriptor("place"));
				tableDescriptor.addFamily(new HColumnDescriptor("date"));
				admin.createTable(tableDescriptor);
			}

			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = new HTable(conf, tableName);

			// Add each person to the table
			// Use the `name` column family for the name
			// Use the `contactinfo` column family for the email

			Put person = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
			person.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
			person.add(Bytes.toBytes("place"), Bytes.toBytes("place"), Bytes.toBytes(place));
			person.add(Bytes.toBytes("date"), Bytes.toBytes("date"), Bytes.toBytes(Instant.now().toString()));
			table.put(person);

			// flush commits and close the table
			table.flushCommits();
			table.close();
			System.out.println("Place saved ");

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return "Bonjour, tout le monde!";
	}
	
	@Path("/saveExpectation")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String hisExpectation(@QueryParam("name") String name, @QueryParam("expectation") String expectation) {
		if ((name == null) || (expectation == null)) {
			return "missing name or place";
		}
		try {
			String tableName = "TableExpectation";
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(tableName)) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
				tableDescriptor.addFamily(new HColumnDescriptor("name"));
				tableDescriptor.addFamily(new HColumnDescriptor("expectation"));
				tableDescriptor.addFamily(new HColumnDescriptor("date"));
				admin.createTable(tableDescriptor);
			}

			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = new HTable(conf, tableName);

			// Add each person to the table
			// Use the `name` column family for the name
			// Use the `contactinfo` column family for the email

			Put person = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
			person.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
			person.add(Bytes.toBytes("expectation"), Bytes.toBytes("expectation"), Bytes.toBytes(expectation));
			person.add(Bytes.toBytes("date"), Bytes.toBytes("date"), Bytes.toBytes(Instant.now().toString()));
			table.put(person);

			// flush commits and close the table
			table.flushCommits();
			table.close();
			System.out.println("expectation saved ");// http://localhost:3031/HelloWorl14.05.v1/api/h/saveExpectation?name=hamza&expectation=succeed

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return "Bonjour, tout le monde!";
	}

}