package com.picoto;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public class Client {

	private String titular = "Titular";
	private TableName tablaTest = TableName.valueOf("Test1");

	public static void main(String[] args) throws IOException {
		new Client().connect();
	}

	private void connect() throws IOException {
		Configuration config = HBaseConfiguration.create();

		String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

		config.addResource(new Path(path));

		try {
			HBaseAdmin.available(config);
		} catch (MasterNotRunningException e) {
			System.out.println("HBase no detectado." + e.getMessage());
			return;
		}

		run(config);

	}

	private void createTable(Admin admin) throws IOException {

		TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tablaTest);
		ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
				.newBuilder(Bytes.toBytes(titular)).setBlocksize(32 * 1024)
				.setCompressionType(Compression.Algorithm.GZ).setDataBlockEncoding(DataBlockEncoding.NONE);
		tableDescBuilder.setColumnFamily(columnDescBuilder.build());
		TableDescriptor desc = tableDescBuilder.build();

		admin.createTable(desc);
	}

	private void deleteTable(Admin admin) throws IOException {
		if (admin.tableExists(tablaTest)) {
			System.out.println("*** Borrando tabla: " + tablaTest.getNameAsString());
			admin.disableTable(tablaTest);
			admin.deleteTable(tablaTest);
		}
	}

	private void get(Table table, String id) throws IOException {
		System.out.println("*** GET ejemplo buscando datos de Titular(nombre, apellidos) para id: " + id);

		Get g = new Get(Bytes.toBytes(id));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes(titular), Bytes.toBytes("nombre"));
		byte[] value2 = r.getValue(Bytes.toBytes(titular), Bytes.toBytes("apellidos"));
		long ts = r.rawCells()[0].getTimestamp();

		System.out.println("Registro encontrado: (" + Bytes.toString(value) + "," + Bytes.toString(value2)
				+ ") con timestamp: " + ts);
		System.out.println("*** Hecho get.");
	}

	private void put(Table table, String id, String nombre, String apellido) throws IOException {

		Put p = new Put(Bytes.toBytes(id));
		p.addColumn(Bytes.toBytes(titular), Bytes.toBytes("nombre"), Bytes.toBytes(nombre));
		p.addColumn(Bytes.toBytes(titular), Bytes.toBytes("apellidos"), Bytes.toBytes(apellido));

		System.out.println("*** PUT ejemplo. Insertando ejemplos de Titular con (Nombre y Apellidos) en la tabla: "
				+ tablaTest.getNameAsString() + " " + p);
		table.put(p);
	}
	
	private void update(Table table, String id, String apellido) throws IOException {

		Put p = new Put(Bytes.toBytes(id));
		p.addColumn(Bytes.toBytes(titular), Bytes.toBytes("apellidos"), Bytes.toBytes(apellido));

		System.out.println("*** UPDATE ejemplo. Actualizando ejemplo de Titular con (Nombre y Apellidos) en la tabla: "
				+ tablaTest.getNameAsString() + " " + p);
		table.put(p);
	}
	
	private void delete(Table table, String id) throws IOException {
		Delete d = new Delete(Bytes.toBytes(id));
		System.out.println("*** DELETE ejemplo. Borrando ejemplo de Titular con (Nombre y Apellidos) en la tabla: "
				+ tablaTest.getNameAsString() + " " + d);
		table.delete(d);
	}

	private void scan(Table table) throws IOException {
		System.out.println("****** SCAN ejemplo recuperando Titular(nombre)");

		Scan scan = new Scan();
		// scan.addColumn(Bytes.toBytes(titular), Bytes.toBytes("nombre"));

		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner) {
				String id = Bytes.toString(result.getRow());
				System.out.println("El scan ha encontrado el siguiente registro con id: " + id);
				get(table, id);
			}
		}
		System.out.println("****** Hecho scan.");
	}

	private void filters(Table table, String id, String apellido) throws IOException {
		System.out.println("****** Buscando con filtro donde id igual: " + id + " y con apellido: " + apellido);
		Filter filter1 = new PrefixFilter(Bytes.toBytes(id));
		Filter filter2 = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(titular)));
		Filter filter3 = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("apellidos")));
		SingleColumnValueFilter filter4 = new SingleColumnValueFilter(Bytes.toBytes(titular),
				Bytes.toBytes("apellidos"), CompareOperator.EQUAL, Bytes.toBytes(apellido));

		List<Filter> filters = Arrays.asList(filter1, filter2, filter3, filter4);

		Scan scan = new Scan();
		scan.setFilter(new FilterList(Operator.MUST_PASS_ALL, filters));

		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner) {
				String idRes = Bytes.toString(result.getRow());
				System.out.println(
						"El Filtro " + scan.getFilter() + " ha encontrado el siguiente registro con id: " + idRes);
				get(table, idRes);
			}
		}
		System.out.println("****** Hecho filtro.");
	}

	public void run(Configuration config) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config)) {

			Admin admin = connection.getAdmin();

			deleteTable(admin);

			if (!admin.tableExists(tablaTest)) {
				createTable(admin);
			}

			Table table = connection.getTable(tablaTest);

			String id1 = "12345678Z";
			String id2 = "87654321X";

			put(table, id1, "Jose Miguel", "Godino");
			put(table, id2, "Elena", "de la Rosa");

			get(table, id1);
			get(table, id2);

			scan(table);
			
			update(table, id1, "Muncharaz");

			filters(table, id1, "Muncharaz");
			
			delete(table, id1);
			
			scan(table);
		}
	}

}