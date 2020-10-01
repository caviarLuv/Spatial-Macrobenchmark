package site.ycsb.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.DBFactory;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.generator.geo.ParameterGenerator;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeatureType;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;

public class CassandraGeomesaClient extends GeoDB {

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
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
			String keyspace = props.getProperty("cassandra.keyspace", "grafittidb");
			String contactpoint = host + ":" + port;

			Map<String, String> parameters = new HashMap<>();
			parameters.put("cassandra.contact.point", "127.0.0.1:9042");
			parameters.put("cassandra.keyspace", keyspace);
			parameters.put("cassandra.catalog", "ycsb");
			for(Entry<String, String> entry: parameters.entrySet()) {
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
		datastore.dispose();
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
	 * Create new schema if no existing SimpleFeatureType for YCSB
	 */
	@Override
	public Status geoLoad(String table, ParameterGenerator generator, Double recordCount) {
		SimpleFeatureType sft = generator.getSimpleFeatureType();

		try {
			if (datastore.getSchema(sft.getTypeName()) == null) {
				System.out.println("No schema is found for " + sft.getTypeName() +
						"\nCreating schema...");
				datastore.createSchema(sft);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	// a geoLoad for loading multiple tables, for macro-benchmark
	@Override
	public Status geoLoad(String table1, String table2, String table3, ParameterGenerator generator,
			Double recordCount) {
		return null;
	}

	/**
	 * Private helper method to convert a GeoJSON Point document into WKT representation
	 * @param geom GeoJSON document
	 * @return spatial data in WKT format
	 */
	private static String convertGeomData(JSONObject geom) {
		String type = geom.getString("type");
		String spatial = "";
		JSONArray coordinates = geom.getJSONArray("coordinates");
		if(type == "Point") {
			spatial = String.format("Point( %f %f)", getX(coordinates), getY(coordinates));
		}
		if(type == "MultiLineString") {
			spatial += "MultiLineString(";
			for(int line = 0; line < coordinates.length(); line++) {
				if(line != 0) {
					spatial += ", ";
				}
				spatial += "(";
				JSONArray points = coordinates.getJSONArray(line);
				for(int point = 0; point < points.length(); point++) {
					if(point != 0) {
						spatial += ", ";
					}
					JSONArray xypt = points.getJSONArray(point);
					spatial += getX(xypt) + " " + getY(xypt);
				}
				spatial += ")";
			}
			spatial += ")";
		}
		return spatial;
	}
	
	private static double getX(JSONArray coordinates) {
		return coordinates.getDouble(0); // x value
	}
	
	private static double getY(JSONArray coordinates) {
		return coordinates.getDouble(1); // y value
	}
	/**
	 * Convert GeoPredicate into Feature to perform insertion through GeoMesa
	 */
	@Override
	public Status geoInsert(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		SimpleFeatureType sft = gen.getSimpleFeatureType();
		// use geotool SimpleFeatureBuilder to create Feature according to FeatureType
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		String key = gen.getGeoPredicate().getDocid();
		String value = gen.getGeoPredicate().getValue();
		JSONObject obj = new JSONObject(value);
		JSONObject properties = null;
		JSONObject geom = null;
		try {
			properties = obj.getJSONObject("properties");
			geom = obj.getJSONObject("geometry");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (properties != null && geom != null) {
			try {
				builder.set("type", obj.getString("type"));
				builder.set("OBJECTID", key);
				builder.set("INCIDENT_NUMBER", properties.getString("INCIDENT_NUMBER"));
				//LOCATION -> _LOCATION due to Java reserved word in geomesa
				builder.set("_LOCATION", properties.getString("LOCATION"));
				builder.set("NOTIFICATION", properties.getString("NOTIFICATION"));
				builder.set("INCIDENT_DATE", properties.getString("INCIDENT_DATE"));
				builder.set("TAG_COUNT", properties.getInt("TAG_COUNT"));
				builder.set("MONIKER_CLASS", properties.getString("MONIKER_CLASS"));
				builder.set("SQ_FT", properties.getInt("SQ_FT"));
				builder.set("PROP_TYPE", properties.getString("PROP_TYPE"));
				if(properties.isNull("Waiver")) {
					builder.set("Waiver", "");
				}
				else {
					builder.set("Waiver", properties.getString("Waiver"));
				}
				builder.set("geometry", convertGeomData(geom));

				// Generate UUID as the fid
				SimpleFeature feature = builder.buildFeature(null);

				geoInsert(sft, feature);
			} catch (Exception e) {
				System.out.println("Error in building SimpleFeature...");
				e.printStackTrace();
				return Status.ERROR;
			}
		} else {
			return Status.ERROR;
		}
		return Status.OK;
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
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.OK;
	}

	// a geoInsert that works on multiple tables for Macro-benchmark
	@Override
	public Status geoInsert(String table, String value, ParameterGenerator gen) {
		return null;
	}

	@Override
	public Status geoUpdate(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		// System.err.println("geoUpdate not implemented");
		return null;
	}

	@Override
	public Status geoNear(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		// System.err.println("geoNear not implemented");
		return null;
	}

	/**
	 * Build Geomesa box CQL and execute query and retrive result
	 * bbox(x1, x2, y1, y2) 
	 */
	@Override
	public Status geoBox(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		String boxField = gen.getGeoPredicate().getNestedPredicateA().getName();
		JSONArray boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA().getJSONArray("coordinates");
	    JSONArray boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA().getJSONArray("coordinates");
	    String filter = String.format("BBOX(%s, %f, %f, %f, %f)", boxField, getX(boxFieldValue1), getX(boxFieldValue2), 
	    		getY(boxFieldValue1), getY(boxFieldValue2));
	    System.out.println(filter);
		try {
			//create query
			Query q = new Query(table, ECQL.toFilter(filter));
			
			//submit query
			FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(q, Transaction.AUTO_COMMIT);

			//Count number of result, removed for benchmark accuracy(?)
			/*
			int count = 0;
			while(reader.hasNext()) {
				count++;
			}
			System.out.printf("Operation returned %d documents.", count);
			*/
		} catch (CQLException e1) {
			System.out.println("Error when creating filter.");
			return Status.ERROR;
		}catch(IOException e2) {
			System.out.println("Failed to read data from Cassandra");
			return Status.ERROR;
		}
		return Status.OK;
	}

	
	@Override
	public Status geoIntersect(String table, HashMap<String, ByteIterator> result, ParameterGenerator gen) {
		String fieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
	    JSONObject intersectFieldValue2 = gen.getGeoPredicate().getNestedPredicateC().getValueA();
	//System.out.println("\n\n\n" + convertGeomData(intersectFieldValue2));
	    String filter = String.format("INTERSECTS(%s, %s)", table, convertGeomData(intersectFieldValue2));
	    System.out.println(filter);
	    try {
	    	//create query
			Query q = new Query(table, ECQL.toFilter(filter));
			
			//submit query
			FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(q, Transaction.AUTO_COMMIT);

	    }catch(CQLException e1) {
	    	System.out.println("Error when creating filter.");
			return Status.ERROR;
	    }catch(IOException e2) {
	    	System.out.println("Failed to read data from Cassandra");
			return Status.ERROR;
	    }
	    return Status.OK;
	}

	@Override
	public Status geoScan(String table, Vector<HashMap<String, ByteIterator>> result, ParameterGenerator gen) {
		// System.err.println("geoScan not implemented");
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status geoUseCase2(String table, HashMap<String, Vector<HashMap<String, ByteIterator>>> result,
			ParameterGenerator gen) {
		// TODO Auto-generated method stub
		return null;
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