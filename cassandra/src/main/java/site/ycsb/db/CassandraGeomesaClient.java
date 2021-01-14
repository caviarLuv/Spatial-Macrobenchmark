package site.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;
import java.util.Random;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.generator.geo.ParameterGenerator;
import site.ycsb.workloads.geo.DataFilter;
import site.ycsb.workloads.geo.GeoWorkload;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.coordinate.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.locationtech.geomesa.process.query.ProximitySearchProcess;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

public class CassandraGeomesaClient extends GeoDB {

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
	
	private static final AtomicInteger PRELOAD_COUNT = new AtomicInteger(1);
	/** Used to include a field in a response. */
	private static final Integer INCLUDE = Integer.valueOf(1);

	private static DataStore datastore;
	private SimpleFeatureType sft;

	// initialie a cassandra geomesa datastore
	@Override
	public void init() {
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
			String host = props.getProperty("host", "localhost");
			String port = props.getProperty("port", "9042");
			String keyspace = props.getProperty("cassandra.keyspace", "testdb");
			String contactpoint = host + ":" + port;

			Map<String, String> parameters = new HashMap<>();
			parameters.put("cassandra.contact.point", contactpoint);
			parameters.put("cassandra.keyspace", keyspace);
			parameters.put("cassandra.catalog", "geoycsb");
			parameters.put("geomesa.query.threads", "5"); //setting number of threads per query
			for (Entry<String, String> entry : parameters.entrySet()) {
				System.out.println("Key=" + entry.getKey() + "   value=" + entry.getValue());
			}
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

	/**
	 * Clean up state and close connection
	 */
	@Override
	public void cleanup() throws DBException {
		if (INIT_COUNT.decrementAndGet() == 0) {
			try {
				datastore.dispose();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			} finally {
				datastore = null;
			}
		}
	}

	private String convertByteToHex(byte[] ary) {
		StringBuilder s = new StringBuilder();
		for (byte b : ary) {
			s.append(String.format("%02x", b));
		}
		return s.toString();
	}

	/**
	 * IMPORTANT!!! This will be called once PER thread, so should not be reading
	 * from a file unless is trying to generate Feature randomly
	 * 
	 * Override YCSB load phase loading data into the datastore through Geomesa
	 * TODO: this method should load data into the datastore via geomesa, therefore,
	 * first need to create the schema (FeatureType) for the data, then generate
	 * Feature either randomly/read from the JSON file. I have a feeling that
	 * ParameterGenerator behave very similar to FeatureType, maybe able to
	 * reuse/modify somecode. Then pass to geoInsert to execute actual insertion.
	 * Objective (tentative): 1. load from json 2. parse data 3. create
	 * SimpleFeatureType 4. create Feature
	 * 
	 */
	@Override
	public Status geoLoad(String table, ParameterGenerator generator, Double recordCount) {
		// check if in the first geoload call, if so, preload original dataset into
		// memcached, synchronized to ensure data is completely loaded prior to synthesis
		synchronized (INCLUDE) {
			if(PRELOAD_COUNT.compareAndSet(1, 0)) {preLoad(table, generator);}
		}

		
		if (geoLoad(table, generator) == Status.ERROR)
			return Status.ERROR;
		generator.incrementSynthesisOffset();
		
		return Status.OK;
	}

	public void preLoad(String table, ParameterGenerator generator) {
		System.out.println("PRELOADING HERE  " + table);
		FeatureJSON io = new FeatureJSON();
		try {
			SimpleFeatureIterator reader = datastore.getFeatureSource(table).getFeatures().features();
			while (reader.hasNext()) {
				SimpleFeature data = reader.next();
				generator.putDocument(table, data.getID(), io.toString(data));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// a geoLoad for loading multiple tables, for macro-benchmark
	@Override
	public Status geoLoad(String table1, String table2, String table3, ParameterGenerator generator,
			Double recordCount) {
		synchronized (INCLUDE) {
			if(PRELOAD_COUNT.compareAndSet(1, 0)) {
				//Pre-populating data into memcached
				preLoad(table1, generator);
				preLoad(table2, generator);
				preLoad(table3, generator);
				}
		}
//		try {
//	      if(geoLoad(table1, generator) == Status.ERROR) {
//	        return Status.ERROR;
//	      }
//	      if(geoLoad(table2, generator) == Status.ERROR) {
//	        return Status.ERROR;
//	      }
//	      if(geoLoad(table3, generator) == Status.ERROR) {
//	        return Status.ERROR;
//	      }
//	      generator.incrementSynthesisOffset();
//	      
//	      return Status.OK;
//	    } catch (Exception e) {
//	      System.err.println(e.toString());
//	    }
	    return Status.ERROR;
	}

	/**
	 * Private helper method to load ALL DOCS of a generic table.
	 * 
	 * @param table
	 * @param generator
	 * @return Status
	 */
	private Status geoLoad(String table, ParameterGenerator generator) {
		try {
			System.out.println("geoloading");
			Random seed = new Random();
			// Load EVERY document of the collection
			for (int i = 0; i < generator.getTotalDocsCount(table); i++) {
				// Get the random document from memcached
				String docKey = generator.getNextId(table);
				String value = generator.getDocument(table, docKey);
				if (value == null) {
					System.out.println(table);
					System.out.println(String.format("OBJECTID=%s", docKey));
					System.out.println("Empty return, Please populate data first.");
					return Status.OK;
				}
				// Generate random id and Synthesize new document
				byte[] fid = new byte[12];
				seed.nextBytes(fid);

				String newDocBody = generator.buildGeoInsertDocument(table, Integer.parseInt(docKey),
						convertByteToHex(fid));
				// Add to database
				geoInsert(table, newDocBody, generator);
				if(table.equals(ParameterGenerator.GEO_DOCUMENT_PREFIX_SCHOOLS)) {         
			          int newKey = Integer.parseInt(docKey) + (generator.getTotalDocsCount(table) * 
			              ((generator.getSynthesisOffsetRows() * ParameterGenerator.getSynthesisOffsetMax()) 
			                  + generator.getSynthesisOffsetCols()));
			          generator.putDocument(table, newKey + "", newDocBody);
			   }
			}

			return Status.OK;

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.toString());

			return Status.ERROR;
		}
	}

	/**
	 * Private helper method to convert a GeoJSON Point document into WKT
	 * representation
	 * 
	 * @param geom GeoJSON document
	 * @return spatial data in WKT format
	 */
	private static String convertGeomData(JSONObject geom) {
		String type = geom.getString("type");
		String spatial = "";
		JSONArray coordinates = geom.getJSONArray("coordinates");
		if (type.equals("Point")) {
			spatial = String.format("Point(%s %s)", getX(coordinates), getY(coordinates));
		}
		if (type.equals("MultiLineString")) {
			spatial += "MultiLineString(";
			for (int line = 0; line < coordinates.length(); line++) {
				if (line != 0) {
					spatial += ", ";
				}
				spatial += "(";
				JSONArray points = coordinates.getJSONArray(line);
				for (int point = 0; point < points.length(); point++) {
					if (point != 0) {
						spatial += ", ";
					}
					JSONArray xypt = points.getJSONArray(point);
					spatial += getX(xypt) + " " + getY(xypt);
				}
				spatial += ")";
			}
			spatial += ")";
		}
		if (type.equals("Polygon")) {
			spatial += "Polygon((";
			coordinates = coordinates.getJSONArray(0);
			for (int i = 0; i < coordinates.length(); i++) {
				if (i != 0) {
					spatial += ", ";
				}
				JSONArray point = coordinates.getJSONArray(i);
				spatial += getX(point) + " " + getY(point);
			}
			spatial += "))";
		}
		return spatial;
	}

	private static String getX(JSONArray coordinates) {
		// return coordinates.getBigDecimal(0) + "";
		return coordinates.getDouble(0) + ""; // x value
	}

	private static String getY(JSONArray coordinates) {
		// return coordinates.getBigDecimal(1) + "";
		return coordinates.getDouble(1) + ""; // y value
	}

	private static SimpleFeature incidentBuilder(SimpleFeatureBuilder builder, String key, JSONObject obj) {
		SimpleFeature feature = null;
		try {
		JSONObject properties = null;
		JSONObject geom = null;
			properties = obj.getJSONObject("properties");
			geom = obj.getJSONObject("geometry");
	
		if (properties != null && geom != null) {
	
				builder.set("type", obj.getString("type"));
				builder.set("OBJECTID", key);
				builder.set("INCIDENT_NUMBER", properties.getString("INCIDENT_NUMBER"));
				// LOCATION -> _LOCATION due to Java reserved word in geomesa
				builder.set("_LOCATION", properties.getString("_LOCATION"));
				builder.set("NOTIFICATION", properties.getString("NOTIFICATION"));
				builder.set("INCIDENT_DATE", properties.getString("INCIDENT_DATE"));
				builder.set("TAG_COUNT", properties.getInt("TAG_COUNT"));
				builder.set("MONIKER_CLASS", properties.getString("MONIKER_CLASS"));
				builder.set("SQ_FT", properties.getInt("SQ_FT"));
				builder.set("PROP_TYPE", properties.getString("PROP_TYPE"));
				builder.set("Waiver", properties.optString("Waiver"));
				builder.set("geometry", convertGeomData(geom));

				// Generate UUID as the fid
				feature = builder.buildFeature(null);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}
	/**
	 * Convert GeoPredicate into Feature to perform insertion through GeoMesa
	 */
	@Override
	public Status geoInsert(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		SimpleFeatureType sft;
		try {
			sft = datastore.getSchema(table);
		} catch (IOException e1) {
			throw new RuntimeException(String.format("Cannot fetch schema-%s from database", table));
		}
		// use geotool SimpleFeatureBuilder to create Feature according to FeatureType
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		String key = gen.getGeoPredicate().getDocid();
		String value = gen.getGeoPredicate().getValue();

		JSONObject obj = new JSONObject(value);
		SimpleFeature feature = null;
		if(table.equals("incidents")) {
			feature = incidentBuilder(builder, key, obj);
		}
		if(table.equals("buildings")) {
			feature = buildingBuilder(builder, key, obj);
		}
		if(table.equals("schools")) {
			feature = schoolBuilder(builder, key, obj);
		}
		if(feature == null) return Status.ERROR;
		
		return geoInsert(sft, feature);
		
	}

	private SimpleFeature schoolBuilder(SimpleFeatureBuilder builder, String key, JSONObject obj) {
		SimpleFeature feature = null;
		try {
		JSONObject properties = null;
		JSONObject geom = null;
			properties = obj.getJSONObject("properties");
			geom = obj.getJSONObject("geometry");
	
		if (properties != null && geom != null) {
	
			builder.set("type", obj.getString("type"));
			builder.set("OBJECTID", properties.optString("OBJECTID"));
			builder.set("Name", properties.optString("Name"));
			builder.set("description", properties.optString("description"));
			builder.set("geometry", convertGeomData(geom));

				// Generate UUID as the fid
				feature = builder.buildFeature(null);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}

	private SimpleFeature buildingBuilder(SimpleFeatureBuilder builder, String key, JSONObject obj) {
		SimpleFeature feature = null;
		try {
		JSONObject properties = null;
		JSONObject geom = null;
			properties = obj.getJSONObject("properties");
			geom = obj.getJSONObject("geometry");
	
		if (properties != null && geom != null) {
			builder.set("type", obj.getString("type"));
			builder.set("OBJECTID", properties.get("OBJECTID"));
			builder.set("TOPELEV_M", properties.optString("TOPELEV_M"));
			builder.set("BASEELEV_M", properties.optString("BASEELEV_M"));
			builder.set("HGT_AGL", properties.optString("HGT_AGL"));
			builder.set("MED_SLOPE", properties.optString("MED_SLOPE"));
			builder.set("ROOFTYPE", properties.optString("ROOFTYPE"));
			builder.set("AVGHT_M", properties.optString("AVGHT_M"));
			builder.set("BASE_M", properties.optString("BASE_M"));
			builder.set("ORIENT8", properties.optString("ORIENT8"));
			builder.set("LEN", properties.optString("LEN"));
			builder.set("WID", properties.optString("WID"));
			builder.set("GlobalID", properties.getString("GlobalID"));
			builder.set("Shape__Area", properties.getDouble("Shape__Area"));
			builder.set("Shape__Length", properties.getDouble("Shape__Length"));
			builder.set("geometry", convertGeomData(geom));

				// Generate UUID as the fid
				feature = builder.buildFeature(null);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}

	public Status geoInsert(String table, String value, ParameterGenerator gen) {
		SimpleFeatureType sft;
		try {
			sft = datastore.getSchema(table);
		} catch (IOException e1) {
			throw new RuntimeException(String.format("Cannot fetch schema-%s from database", table));
		}
		// use geotool SimpleFeatureBuilder to create Feature according to FeatureType
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		SimpleFeature feature = null;
		JSONObject obj = new JSONObject(value);
		String key = obj.getString("id");
		if(table.equals("incidents")) {
			feature = incidentBuilder(builder, key, obj);
		}
		if(table.equals("buildings")) {
			feature = buildingBuilder(builder, key, obj);
		}
		if(table.equals("schools")) {
			feature = schoolBuilder(builder, key, obj);
		}
		if(feature == null) return Status.ERROR;
		
		return geoInsert(sft, feature);
	}

	/**
	 * Helper function to write in geomesa
	 * 
	 * @param sft
	 * @param feature
	 * @return
	 */
	public Status geoInsert(SimpleFeatureType sft, SimpleFeature feature) {
		try {
			System.out.println("Writing data...");
			FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(sft.getTypeName(),
					Transaction.AUTO_COMMIT);
			SimpleFeature toWrite = writer.next();
			toWrite.setAttributes(feature.getAttributes());
			toWrite.getUserData().putAll(feature.getUserData());
			writer.write();

			 System.out.println("Insert +++++++ " + DataUtilities.encodeFeature(toWrite));
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	@Override
	public Status geoUpdate(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {

		try {
			Random rand = new Random();
			int key = rand.nextInt(
					(Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) - Integer.parseInt(GeoWorkload.DOCS_START_VALUE))
							+ 1)
					+ Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
			String updateFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
			String updateFieldValue = convertGeomData(gen.getGeoPredicate().getNestedPredicateA().getValueA());

			Filter filter = ECQL.toFilter(String.format("OBJECTID=%s", (key + "")));
			FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriter(table, filter,
					Transaction.AUTO_COMMIT);
			if (!writer.hasNext()) {
				System.err.println("Document not found");
				return Status.NOT_FOUND;
			} else {
				SimpleFeature tobemodified = writer.next();
				tobemodified.setAttribute(updateFieldName, updateFieldValue);
				writer.write();
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	
	@Override
	public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		String nearFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
		String dockey = gen.getGeoPredicate().getDocid();
		String nearFieldValue = convertGeomData(gen.getGeoPredicate().getNestedPredicateA().getValueA());
		String filter = String.format("DWITHIN(%s, %s, 1000, meters)", nearFieldName, nearFieldValue);
		//String filter = "DWITHIN(geometry, POINT(-111.95486393503784 33.40702744850014), 1000, meters)";
		TreeMap<Double, SimpleFeature> map = new TreeMap<>();
		try {
			//query database
			SimpleFeatureSource s = datastore.getFeatureSource(table);
			SimpleFeatureCollection c = s.getFeatures(ECQL.toFilter(filter));

			//sort
			SimpleFeatureIterator reader = c.features();
			if (reader.hasNext()) {
		        WKTReader wkt = new WKTReader();
		        int count = 0;
				while(reader.hasNext()) {
					SimpleFeature f = reader.next();
					count ++;
					//compute distance to the nearField
					Point point1 = (Point) wkt.read(f.getDefaultGeometry().toString());
					Point point2 = (Point) wkt.read(nearFieldValue);
					map.put(point1.distance(point2), f);
				}
				Map.Entry<Double, SimpleFeature> entry = map.firstEntry();
				//System.out.println("##"+DataUtilities.encodeFeature(entry.getValue())+"\ncount="+count+nearFieldValue);
				geoFillMap(result, entry.getValue());
				reader.close();
				return Status.OK;
				
			} else {
				reader.close();
				return Status.NOT_FOUND;
			}
		}catch(Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}
	
//	@Override
//	public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
//		//KNearestNeighborSearchProcess process = new KNearestNeighborSearchProcess();
//		ProximitySearchProcess tp = new ProximitySearchProcess();
//		//// String nearFieldName =
//		//// gen.getGeoPredicate().getNestedPredicateA().getName();
//		String dockey = gen.getGeoPredicate().getDocid();
//		//JSONObject nearFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();
//		
//		//// System.out.println(nearFieldName + ", " + nearFieldValue.toString());
//		//SimpleFeatureCollection input = new SpatialIndexFeatureCollection();
//		
//		// convert json to simple feature
//		SimpleFeatureType sft;
//		try {
//			sft = datastore.getSchema(table);
//		} catch (IOException e1) {
//			throw new RuntimeException(String.format("Cannot fetch schema-%s from database", table));
//		}
//		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
//		builder.set("OBJECTID", dockey);
//		//builder.set("geometry", convertGeomData(nearFieldValue));
//		builder.set("geometry", "Point(-111.8802089256232 33.387247158935985)");
//		SimpleFeature feature = builder.buildFeature(dockey);
//		//((SpatialIndexFeatureCollection) input).add(feature);
//		SimpleFeatureCollection input = DataUtilities.collection(feature);
//		// Obtain dataset
//		SimpleFeatureCollection data = null;
//		try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(new Query(table),
//				Transaction.AUTO_COMMIT)) {
//			data = DataUtilities.collection(reader);
//		} catch (IOException e) {
//			e.printStackTrace();
//			return Status.ERROR;
//		};
//		
////		try {
////			SimpleFeatureSource s = datastore.getFeatureSource(table);
////			data = s.getFeatures();
////		} catch (IOException e) {
////			e.printStackTrace();
////			return Status.ERROR;
////		};
//
//		long start = System.nanoTime();
//		//SimpleFeatureCollection results = process.execute(input, data, 140, 0.0, 1000.0);
//		SimpleFeatureCollection results = tp.execute(input, data, 1000.0);
//		System.out.println("**" + (System.nanoTime()-start)/1000000L);
//		SimpleFeatureIterator iterator = results.features();
//		//int count = 0;
//		try {
//			if (iterator.hasNext()) {
//				/*
//				while(iterator.hasNext()) {
//				SimpleFeature f = iterator.next();
//				System.out.println(DataUtilities.encodeFeature(f));
//				count++;
//				}
//				System.out.println(count);
//				//geoFillMap(result, f);	 
//			 */
//				return Status.OK;
//			} else {
//				return Status.NOT_FOUND;
//			}
//		} finally {
//			iterator.close();
//		}
//	}

	/**
	 * Build Geomesa box CQL and execute query and retrive result bbox(x1, x2, y1,
	 * y2)
	 */
	@Override
	public Status geoBox(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		String boxField = gen.getGeoPredicate().getNestedPredicateA().getName();
		JSONArray boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA().getJSONArray("coordinates");
		JSONArray boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA().getJSONArray("coordinates");
		String filter = String.format("BBOX(%s, %s, %s, %s, %s)", boxField, getX(boxFieldValue1), getY(boxFieldValue1), 
				getX(boxFieldValue2), getY(boxFieldValue2));
		System.out.println(filter);
		try {
			SimpleFeatureSource s = datastore.getFeatureSource(table);
			// create query
			//Query q = new Query(table, ECQL.toFilter(filter));
			//long start = System.nanoTime();
			// submit query
			SimpleFeatureCollection c = s.getFeatures(ECQL.toFilter(filter));

			// Count number of result, removed for benchmark accuracy
			/*
			 * int count = 0; while(reader.hasNext()) { count++; }
			 * 
			 * System.out.printf("Operation returned %d documents.", count);
			 */

			//System.out.println("\n"+(end-start)+ "ns");
			
			SimpleFeatureIterator reader = c.features();
			if (reader.hasNext()) {
				geoFillMap(result, reader.next());
				reader.close();
				return Status.OK;
			} else {
				reader.close();
				return Status.NOT_FOUND;
			}

		} catch (CQLException e1) {
			System.out.println("Error when creating filter.");
			return Status.ERROR;
		} catch (IOException e2) {
			System.out.println("Failed to read data from Cassandra");
			return Status.ERROR;
		}
	}

	@Override
	public Status geoIntersect(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		String fieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
		JSONObject intersectFieldValue2 = gen.getGeoPredicate().getNestedPredicateC().getValueA();
		String filter = String.format("INTERSECTS(%s, %s)", fieldName1, convertGeomData(intersectFieldValue2));
		// String filter = String.format("INTERSECTS(%s, %s)", fieldName1,
		// "LineString( -111.94157702764069 33.4300795967036, -111.60 33.4300795967036)");
		
		try {
			SimpleFeatureSource s = datastore.getFeatureSource(table);
			
			// create query
			//Query q = new Query(table, ECQL.toFilter(filter));

			SimpleFeatureCollection c = s.getFeatures(ECQL.toFilter(filter));
			SimpleFeatureIterator reader = c.features();

			//FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(q,Transaction.AUTO_COMMIT);
			if (reader.hasNext()) {
				//System.out.println(DataUtilities.encodeFeature(reader.next()));
				geoFillMap(result, reader.next());
				reader.close();
				return Status.OK;
			}
			
			reader.close();
			return Status.NOT_FOUND;
		} catch (CQLException e1) {
			System.out.println("Error when creating filter.");
			return Status.ERROR;
		} catch (IOException e2) {
			System.out.println("Failed to read data from Cassandra");
			return Status.ERROR;
		}
	}

	@Override
	public Status geoScan(String table, Vector<HashMap<String, ByteIterator>> result, ParameterGenerator gen) {
		String startkey = gen.getIncidentIdWithDistribution();
		int recordcount = gen.getRandomLimit();
		try {
			Query query = new Query(table, ECQL.toFilter(String.format("OBJECTID=%s", startkey)));
			query.setMaxFeatures(recordcount);
			FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(query,
					Transaction.AUTO_COMMIT);

			if (!reader.hasNext()) {
				System.err.println("Nothing found in scan for key " + startkey);
				return Status.ERROR;
			}
			result.ensureCapacity(recordcount);

			while (reader.hasNext()) {
				HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

				SimpleFeature obj = reader.next();
				geoFillMap(resultMap, obj);
				result.add(resultMap);
			}
			reader.close();
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

//need to test
	protected void geoFillMap(Map<String, ByteIterator> resultMap, SimpleFeature obj) {
		String[] fieldNames = DataUtilities.attributeNames(obj.getFeatureType());
		for (String key : fieldNames) {
			String value = "null";
			if (obj.getAttribute(key) != null) {
				value = obj.getAttribute(key).toString();
			}
			resultMap.put(key, new StringByteIterator(value));
		}
	}

	/**
	 * Not use in GeoYCSB
	 */
	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Not use in GeoYCSB
	 */
	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Not use in GeoYCSB
	 */
	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Not use in GeoYCSB
	 */
	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Not use in GeoYCSB
	 */
	@Override
	public Status delete(String table, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * ========================= Macro benchmark
	 * ==============================================
	 */
	@Override
	public Status geoUseCase1(String table, HashMap<String, Vector<HashMap<String, ByteIterator>>> result,
			ParameterGenerator gen) {
		try {
		      
		      int maxGraffitiCount = Integer.MIN_VALUE;
		      String maxGraffitiSchool = "";
		      Vector<HashMap<String, ByteIterator>> maxGraffiti = null;
		      
		      // Perform near query on incidents for all school documents
		      for(DataFilter school : gen.getGeometryPredicatesList()) {
		        String nearFieldName = school.getNestedPredicateA().getName();
		       
		        KNearestNeighborSearchProcess process = new KNearestNeighborSearchProcess();
				JSONObject nearFieldValue = school.getNestedPredicateA().getValueA();
				
				SimpleFeatureCollection input = new SpatialIndexFeatureCollection();
				// convert json to simple feature
				SimpleFeatureType sft;
				try {
					sft = datastore.getSchema(table);
				} catch (IOException e1) {
					throw new RuntimeException(String.format("Cannot fetch schema-%s from database", table));
				}
				SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
				builder.set("OBJECTID", school.getDocid());
				builder.set(nearFieldName, convertGeomData(nearFieldValue));
				SimpleFeature feature = builder.buildFeature(school.getDocid());
				System.out.println("near query: " + DataUtilities.encodeFeature(feature));
				((SpatialIndexFeatureCollection) input).add(feature);
				// Obtain dataset
				SimpleFeatureCollection data = null;
				try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(new Query(table),
						Transaction.AUTO_COMMIT)) {
					data = DataUtilities.collection(reader);
				} catch (IOException e) {
					e.printStackTrace();
					return Status.ERROR;
				}
				;

				SimpleFeatureCollection results = process.execute(input, data, 10, 0.0, 100000.0);
				SimpleFeatureIterator iterator = results.features();

				
				
		        // If no graffiti was found near the school, the school gets an empty vector
				/*
		        if(!iterator.hasNext()) {
		          result.put(school.getName(), new Vector<HashMap<String, ByteIterator>>());
		          continue;
		        }*/
		        
		        // If there is graffiti, add the results under the school's name
		        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
		        while (iterator.hasNext()) {
		          HashMap<String, ByteIterator> resultMap =
		              new HashMap<String, ByteIterator>();

		          SimpleFeature f = iterator.next();
		          geoFillMap(resultMap, f);
		          graffitiResults.add(resultMap);
		        }
		        if(graffitiResults.size() > maxGraffitiCount) {
		          maxGraffitiSchool = school.getName() + school.getNestedPredicateA().getValueA().toString();
		          maxGraffiti = graffitiResults;
		          maxGraffitiCount = maxGraffiti.size();

		          System.out.println(maxGraffitiSchool + " graffiti count: " + maxGraffiti.size());
		        } else {
		          System.out.println("PASS...");
		        }
//		        result.put(school.getName(), graffitiResults);
		      }
		      
		      if(maxGraffiti == null) {
		        return Status.ERROR;
		      }
		      result.put(maxGraffitiSchool, maxGraffiti);
		      return Status.OK;
		      
		    } catch (Exception e) {
		      e.printStackTrace();
		      return Status.ERROR;
		    }
	}

	@Override
	public Status geoUseCase2(String table, HashMap<String, Vector<HashMap<String, ByteIterator>>> result,
			ParameterGenerator gen) {
		try {
		      // Loop through grid of city
		      for(DataFilter cell : gen.getGeometryPredicatesList()) {
		        String fieldName = cell.getName();
		        JSONObject intersectFieldValue = cell.getValueA();
		        
				JSONArray boxField = intersectFieldValue.getJSONArray("coordinates");
				System.out.println(boxField);
				/*
				String filter = String.format("BBOX(%s, %s, %s, %s, %s)", fieldName, getX(boxField), getY(boxField));

					// create query
					Query q = new Query(table, ECQL.toFilter(filter));

					// submit query
					FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(q,
							Transaction.AUTO_COMMIT);


		        if(!reader.hasNext()) {
		          result.put(intersectFieldValue.toString(), new Vector<HashMap<String, ByteIterator>>());
		          System.out.println("No graffiti in this cell " + intersectFieldValue.toString());
		          continue;
		        }

		        // If there is graffiti, add the results under the cell's locations
		        Vector<HashMap<String, ByteIterator>> graffitiResults = new Vector<>();
		        while (reader.hasNext()) {
		          HashMap<String, ByteIterator> resultMap =
		              new HashMap<String, ByteIterator>();

		          SimpleFeature f = reader.next();
		          geoFillMap(resultMap, f);
		          graffitiResults.add(resultMap);
		        }
		        result.put(intersectFieldValue.toString(), graffitiResults);
		        System.out.println(intersectFieldValue.toString() + ": COUNT = " + graffitiResults.size());
		        */
		      }
		      return Status.OK;
		    } catch (Exception e) {
		      e.printStackTrace();
		      return Status.ERROR;
		    }
	}

	@Override
	public Status geoUseCase3(String table1, String table2,
			HashMap<String, Vector<HashMap<String, ByteIterator>>> result, ParameterGenerator gen) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status geoUseCase4(String table, String operation, Set<Integer> deleted, ParameterGenerator gen) {
		// TODO Auto-generated method stub
		return null;
	}
}