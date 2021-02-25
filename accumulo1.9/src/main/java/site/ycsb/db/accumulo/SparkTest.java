package site.ycsb.db.accumulo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import site.ycsb.Status;

public class SparkTest {
//	"POLYGON (())"
	private static String[] polygons = {
			"POLYGON ((-72.2793102 42.9257837, -72.2793102 42.9242832, -72.2780657 42.9242832, -72.2788274 42.9249902, -72.2784090 42.9258151, -72.2793102 42.9257837))",
			"POLYGON ((-72.2772074 42.9258387, -72.2784090 42.9258151, -72.2788274 42.9249902, -72.2780657 42.9242832, -72.2777009 42.9249745, -72.2764349 42.9246760, -72.2772074 42.9258387))",
			"POLYGON ((-72.2769713 42.9234976, -72.2764349 42.9246760, -72.2777009 42.9249745, -72.2780657 42.9242832, -72.2788274 42.9249902, -72.2787738 42.9236075, -72.2769713 42.9234976))",
			"POLYGON ((-72.2762418 42.9302379, -72.2762632 42.9298137, -72.2757053 42.9295781, -72.2749114 42.9297666, -72.2762418 42.9302379))",
			"POLYGON ((-72.2834730 42.9266714, -72.2851253 42.9224919, -72.2774005 42.9214706, -72.2739029 42.9236861, -72.2758985 42.9278812, -72.2834730 42.9266714))",
			"POLYGON ((-72.2752064 42.9297607, -72.2748819 42.9294602, -72.2747612 42.9300514, -72.2749114 42.9297666, -72.2752064 42.9297607 ))"
			};
	
	private static void ingest(DataStore ds) {
		try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
		          ds.getFeatureWriterAppend("polygon", Transaction.AUTO_COMMIT)) {
			for (String geom: polygons) {
				SimpleFeature next = writer.next();
				next.setAttribute("geom", geom);
				writer.write();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void query(Map<String, String> parameters, String table, String filter) {
		
		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
				.master("local[*]").getOrCreate();
		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
				.option("geomesa.feature", table).load();
		System.out.println(table);
		dataFrame.createOrReplaceTempView(table);
		
		String field = "geom";
		String wktGeom = "POLYGON ((-72.2793102 42.9257837, -72.2793102 42.9242832, -72.2780657 42.9242832, -72.2788274 42.9249902, -72.2784090 42.9258151, -72.2793102 42.9257837))";
		//String wktGeom = "MULTILINESTRING ((-72.2798145 42.9251002, -72.2760057 42.9244639, -72.2755122 42.9251552, -72.2750938 42.9237764))";
		// Query
		String sqlQuery = "select * from %s where %s(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, filter, wktGeom, field));
		System.out.println(String.format("Query %s: %s ::: %s", table, filter, wktGeom));
		resultDataFrame.show(false);

		
	}
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = new HashMap<>();
		parameters.put("accumulo.instance.id", "testdb");
		parameters.put("accumulo.zookeepers", "127.0.0.1:2181");
		parameters.put("accumulo.user", "myUser");
		parameters.put("accumulo.password", "geomesa");
		parameters.put("accumulo.catalog", "myNamespace.geomesa");
		DataStore datastore = null;
		try {
			datastore = DataStoreFinder.getDataStore(parameters);

			if (datastore == null) {
				throw new RuntimeException("Cannot create datastore given the parameter");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/* INGEST */
		//ingest(datastore);
		
		/* QUERY 
		 * equal | check | yes | return itself 
		 * intersect | maybe | yes | inter with itself too
		 * disjoint | check | yes
		 * covers | check | return itself  (?)
		 * crosses | does not cross with polygon(?) | test line | yes
		 * within | check | yes
		 * overlaps | yes | 
		 * touches | check | yes
		 */
		query(parameters, "polygon", "st_intersects");
	}
}
