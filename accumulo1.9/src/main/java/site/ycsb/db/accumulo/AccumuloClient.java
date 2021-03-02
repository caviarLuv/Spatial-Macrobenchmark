/**
 * Copyright (c) 2011 YCSB++ project, 2014-2016 YCSB contributors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.accumulo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.SparkSession;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.generator.geo.ParameterGenerator;

/**
 * <a href="https://accumulo.apache.org/">Accumulo</a> binding for YCSB.
 */
public class AccumuloClient extends GeoDB {

	private static DataStore datastore;
	private static SparkSession sparkSession;
	private static Dataset<Row> countiesFrame;
	private static Dataset<Row> routesFrame;
	private static Map<String, String> parameters;
	// private static SparkSession sparkSession;
	private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

	private static final AtomicInteger PRELOAD_COUNT = new AtomicInteger(1);
	/** Used to include a field in a response. */
	private static final Integer INCLUDE = Integer.valueOf(1);

	@Override
	public void init() throws DBException {
		// Keep track of number of calls to init (for later cleanup)
		INIT_COUNT.incrementAndGet();
		// Synchronized so that we only have a single
		// cluster/session instance for all the threads.
		synchronized (INCLUDE) {

			// Check if the cluster has already been initialized
			if (datastore != null) {
				return;
			}

			Properties props = getProperties();
			String zookeepers = props.getProperty("zookeepers", "127.0.0.1:2181");
			String user = props.getProperty("user", "myUser");
			String pw = props.getProperty("password", "geomesa");
			String catalog = props.getProperty("catalog", "myNamespace.geomesa");
			String instance = props.getProperty("instance", "geoycsb");

			parameters = new HashMap<>();
			parameters.put("accumulo.instance.id", instance);
			parameters.put("accumulo.zookeepers", zookeepers);
			parameters.put("accumulo.user", user);
			parameters.put("accumulo.password", pw);
			parameters.put("accumulo.catalog", catalog);
			// parameters.put("geomesa.security.auths", "USER,ADMIN");
			for (Entry<String, String> entry : parameters.entrySet()) {
				System.out.println("Key=" + entry.getKey() + "   value=" + entry.getValue());
			}

			sparkSession = SparkSession.builder().appName("testSpark").master("local[*]").getOrCreate();
			sparkSession.sparkContext().setLogLevel("ERROR");
			countiesFrame = sparkSession.read().format("geomesa").options(parameters)
					.option("geomesa.feature", "counties").load();
			routesFrame = sparkSession.read().format("geomesa").options(parameters).option("geomesa.feature", "routes")
					.load();
			// create datastore
			try {
				datastore = DataStoreFinder.getDataStore(parameters);

				if (datastore == null) {
					throw new RuntimeException("Cannot create datastore given the parameter");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public void cleanup() throws DBException {
		if (INIT_COUNT.decrementAndGet() == 0) {
			try {
				// datastore.dispose();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			} finally {
				datastore = null;
			}
		}
	}

	public void preLoad(String table, ParameterGenerator generator) {
		System.out.println("PRELOADING HERE  " + table);
		FeatureJSON io = new FeatureJSON(new GeometryJSON(12)); // set precision
		try {
			SimpleFeatureIterator reader = datastore.getFeatureSource(table).getFeatures().features();
			int count = 0;
			while (reader.hasNext()) {
				SimpleFeature data = reader.next();
				generator.putOriginalDocument(table, io.toString(data));
				// System.out.println(io.toString(data));
				count++;
			}
			System.out.println("table size: " + count);
			// for testing a smaller dataset
//			for (int i = 0; i < 100; i++) {
//				SimpleFeature data = reader.next();
//				System.out.println("###original: " + DataUtilities.encodeFeature(data));
//				//System.out.println("143#:" + io.toString(data));
//				generator.putOriginalDocument(table, io.toString(data));
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Status geoLoad(String table, ParameterGenerator generator, Double recordCount) {
		return Status.OK;
	}

	@Override
	public Status geoLoad(String table1, String table2, ParameterGenerator generator, Double recordCount) {
		synchronized (INCLUDE) {
			if (PRELOAD_COUNT.compareAndSet(1, 0)) {
				// Pre-populating data into memcached
				preLoad(table1, generator);
				preLoad(table2, generator);
			}
		}
		try {

			System.out.println(table1 + " " + table2 + "\n\n\n\n\n\n\n\n\n\n\n");
			if (geoLoad(table1, generator) == Status.ERROR) {
				return Status.ERROR;
			}
			if (geoLoad(table2, generator) == Status.ERROR) {
				return Status.ERROR;
			}
			generator.incrementSynthesisOffset();

			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
		}
		return Status.ERROR;
	}

	private Status geoLoad(String table, ParameterGenerator generator) {
		try {
			System.out.println("geoloading");
			// Load EVERY document of the collection
			SimpleFeatureType sft = datastore.getSchema(table);
			FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(sft.getTypeName(),
					Transaction.AUTO_COMMIT);
			// i < generator.getTotalDocsCount(table)
			for (int i = 0; i < generator.getTotalDocsCount(table); i++) {
				// Get the random document from memcached
				String value = generator.getOriginalDocument(table, i + "");
				if (value == null) {
					System.out.println(table);
					System.out.println(String.format("OBJECTID=%s", i));
					System.out.println("Empty return, Please populate data first.");
					return Status.OK;
				}

				/* Synthesize */
				String newDocBody = generator.buildGeoLoadDocument(table, i); // {..., geometry: "Polygon(...)"} WKT
																				// format

				// Add to database
				if (geoInsert(table, newDocBody, sft, writer) == Status.ERROR) {
					return Status.ERROR;
				}

			}
			writer.close();
			return Status.OK;

		} catch (Exception e) {
			e.printStackTrace();

			return Status.ERROR;
		}
	}

	public Status geoInsert(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		System.err.println("geoInsert not implemented");
		return null;
	}

	// a geoInsert that works on multiple tables
	public Status geoInsert(String table, String value, SimpleFeatureType sft,
			FeatureWriter<SimpleFeatureType, SimpleFeature> writer) {
		try {
			SimpleFeature f = null;
			if (table == "counties") {
				f = createCounty(sft, value);
			}
			if (table == "routes") {
				f = createRoute(sft, value);
			}
			if (f != null) {

				SimpleFeature toWrite = writer.next();
				toWrite.setAttributes(f.getAttributes());
				toWrite.getUserData().putAll(f.getUserData());
				writer.write();
				// System.out.println("wrote: " + DataUtilities.encodeFeature(f));
			}
		} catch (IOException e) {
			System.out.println("Schema is not in database, please pre-populate data.");
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	private SimpleFeature createCounty(SimpleFeatureType sft, String value) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		SimpleFeature feature = null;
		try {
			JSONObject obj = new JSONObject(value);
			JSONObject properties = obj.getJSONObject("properties");

			if (obj != null) {
				builder.set("type", obj.getString("type"));
				builder.set("_id", properties.getInt("_id"));
				builder.set("N03_001", properties.optString("N03_001"));
				builder.set("N03_002", properties.optString("N03_002"));
				builder.set("N03_003", properties.optString("N03_003"));
				builder.set("N03_004", properties.optString("N03_004"));
				builder.set("N03_007", properties.optString("N03_007"));
				builder.set("_color", properties.optString("_color"));
				builder.set("_opacity", properties.getInt("_opacity"));
				builder.set("_weight", properties.getInt("_weight"));
				builder.set("_fillColor", properties.optString("_fillColor"));
				builder.set("_fillOpacity", properties.getDouble("_fillOpacity"));
				builder.set("geometry", obj.getString("geometry"));

				// Generate UUID as the fid
				feature = builder.buildFeature(null);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}

	private SimpleFeature createRoute(SimpleFeatureType sft, String value) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		SimpleFeature feature = null;
		try {
			JSONObject obj = new JSONObject(value);
			JSONObject properties = obj.getJSONObject("properties");

			if (obj != null) {

				builder.set("type", obj.getString("type"));
				builder.set("N07_001", properties.getInt("N07_001"));
				builder.set("N07_002", properties.optString("N07_002"));
				builder.set("N07_003", properties.optString("N07_003"));
				builder.set("N07_004", properties.getInt("N07_004"));
				builder.set("N07_005", properties.getInt("N07_005"));
				builder.set("N07_006", properties.getInt("N07_006"));
				builder.set("N07_007", properties.optString("N07_007"));
				builder.set("geometry", obj.getString("geometry"));

				// Generate UUID as the fid
				feature = builder.buildFeature(null);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}

	public Status geoUpdate(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		System.err.println("geoUpdate not implemented");
		return null;
	}

	public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		System.err.println("geoNear not implemented");
		return null;
	}

	public Status geoBox(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {

		return Status.OK;
	}

	/* DE-9IM Methods */
	public Status geoIntersect(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
		// Create DataFrame using the "geomesa" format
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateB().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateB().getValue();

		// Query
		String sqlQuery = "select * from %s where st_intersects(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Intesect ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));

			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}

	}

	public Status geoDisjoint(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		// System.out.println(table);
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateB().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateB().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Disjoint(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Disjoint ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoTouches(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateB().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateB().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Touches(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Touches ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoCrosses(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateA().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateA().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Crosses(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Crosses ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			//// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoWithin(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateA().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateA().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Within(%s, st_geomFromWKT('%s'))";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, field, wktGeom));
		// System.out.println(String.format("Query %s: Within ::: %s", table, wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoContains(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateA().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateA().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Contains(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Contains ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoOverlaps(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		countiesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateC().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateC().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Overlaps(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Overlaps ::: %s", table,
		// wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoEquals(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateB().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateB().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Equals(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Equals ::: %s", table, wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoCovers(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		SparkSession sparkSession = SparkSession.builder().appName("testSpark").config("spark.sql.crossJoin.enabled", "true")
//				.master("local[*]").getOrCreate();
//		Dataset<Row> dataFrame = sparkSession.read().format("geomesa").options(parameters)
//				.option("geomesa.feature", table).load();
		routesFrame.createOrReplaceTempView(table);

		String field = gen.getGeoPredicate().getNestedPredicateA().getName();
		String wktGeom = gen.getGeoPredicate().getNestedPredicateA().getValue();

		// Query
		String sqlQuery = "select * from %s where st_Covers(st_geomFromWKT('%s'), %s)";
		Dataset<Row> resultDataFrame = sparkSession.sql(String.format(sqlQuery, table, wktGeom, field));
		// System.out.println(String.format("Query %s: Covers ::: %s", table, wktGeom));
		try {

			Row first = resultDataFrame.first();
			// System.out.println(first.mkString(" | "));
			return Status.OK;

		} catch (NoSuchElementException e) {
			return Status.NOT_FOUND;
		}
	}

	public Status geoScan(String table, Vector<HashMap<String, ByteIterator>> result, ParameterGenerator gen) {
		System.err.println("geoScan not implemented");
		return null;
	}

	/* Non-Geo functions not used */
	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		// not use in geoycsb
		return null;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		// not use in geoycsb
		return null;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		// not use in geoycsb
		return null;
	}

	@Override
	public Status delete(String table, String key) {
		// not use in geoycsb
		return null;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return null;
	}

}
