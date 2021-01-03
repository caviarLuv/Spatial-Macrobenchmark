package site.ycsb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.Random;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;

import org.opengis.feature.simple.SimpleFeature;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;

public class GeomesaHelper {
	public static ArrayList<SimpleFeature> generateFeatures(SimpleFeatureType sft, String file) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		ArrayList<SimpleFeature> f = new ArrayList<>();
		String table = sft.getTypeName();
		try {
			Scanner reader = new Scanner(new File(file));
			while (reader.hasNext()) {
				JSONObject json = new JSONObject(reader.nextLine());
				if (table.equals("incidents")) {
					f.add(incidentData(builder, json));
				}
				if (table.equals("buildings")) {
					f.add(buildingData(builder, json));
				}
				if (table.equals("schools")) {
					f.add(schoolData(builder, json));
				}
				// System.out.println(json);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return f;
	}

	private static SimpleFeature buildingData(SimpleFeatureBuilder builder, JSONObject obj) {
		SimpleFeature f = null;
		JSONObject properties = obj.getJSONObject("properties");
		JSONObject geom = obj.getJSONObject("geometry");
		try {
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
			f = builder.buildFeature(properties.get("OBJECTID") + "");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error when converting data into SimpleFeature.");
		}
		return f;
	}

	private static SimpleFeature schoolData(SimpleFeatureBuilder builder, JSONObject obj) {
		SimpleFeature f = null;
		JSONObject properties = obj.getJSONObject("properties");
		JSONObject geom = obj.getJSONObject("geometry");
		try {
			builder.set("type", obj.getString("type"));
			builder.set("OBJECTID", properties.optString("OBJECTID"));
			builder.set("Name", properties.optString("Name"));
			builder.set("description", properties.optString("description"));
			builder.set("geometry", convertGeomData(geom));

			f = builder.buildFeature(properties.get("OBJECTID") + "");
		} catch (Exception e) {
			System.err.println("Error when converting data into SimpleFeature.");
		}
		return f;
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

	private static SimpleFeature incidentData(SimpleFeatureBuilder builder, JSONObject obj) {
		SimpleFeature f = null;
		JSONObject properties = obj.getJSONObject("properties");
		JSONObject geom = obj.getJSONObject("geometry");
		try {
			builder.set("type", obj.getString("type"));
			builder.set("OBJECTID", properties.get("OBJECTID"));
			builder.set("INCIDENT_NUMBER", properties.getString("INCIDENT_NUMBER"));
			// LOCATION -> _LOCATION due to Java reserved word in geomesa
			builder.set("_LOCATION", properties.getString("LOCATION"));
			builder.set("NOTIFICATION", properties.getString("NOTIFICATION"));
			builder.set("INCIDENT_DATE", properties.getString("INCIDENT_DATE"));
			builder.set("TAG_COUNT", properties.getInt("TAG_COUNT"));
			builder.set("MONIKER_CLASS", properties.getString("MONIKER_CLASS"));
			builder.set("SQ_FT", properties.getInt("SQ_FT"));
			builder.set("PROP_TYPE", properties.getString("PROP_TYPE"));
			if (properties.isNull("Waiver")) {
				builder.set("Waiver", "");
			} else {
				builder.set("Waiver", properties.getString("Waiver"));
			}
			builder.set("geometry", convertGeomData(geom));

			f = builder.buildFeature(properties.get("OBJECTID") + "");
		} catch (Exception e) {
			System.err.println("Error when converting data into SimpleFeature.");
		}
		return f;
	}

	public static SimpleFeatureType createSchema(String table) {
		SimpleFeatureType sft = null;
		if (table.equals("incidents")) {

			sft = incidentSchema();
		}
		if (table.equals("buildings")) {

			sft = buildingSchema();
		}
		if (table.equals("schools")) {

			sft = schoolSchema();
		}
		if (sft == null)
			throw new RuntimeException("Error encounter when creating schema for " + table);

		return sft;
	}

	public static SimpleFeatureType incidentSchema() {
		SimpleFeatureType sft = null;
		StringBuilder attributes = new StringBuilder();
		attributes.append("type" + ":String,");
		attributes.append("OBJECTID" + ":String,");
		attributes.append("INCIDENT_NUMBER" + ":String,");
		// Modify LOCATION -> _LOCATION due to geomesa reserved word list
		attributes.append("_" + "LOCATION" + ":String,");
		attributes.append("NOTIFICATION" + ":String,");
		attributes.append("INCIDENT_DATE" + ":String,");
		attributes.append("TAG_COUNT" + ":Integer,");
		attributes.append("MONIKER_CLASS" + ":String,");
		attributes.append("SQ_FT" + ":Integer,");
		attributes.append("PROP_TYPE" + ":String,");
		attributes.append("Waiver" + ":String,");
		attributes.append("*" + "geometry" + ":Point:srid=4326");
		sft = SimpleFeatureTypes.createType("incidents", attributes.toString());
		// Only enabling z2 indexing
		sft.getUserData().put("geomesa.indices.enabled", "z2");
		return sft;
	}

	public static SimpleFeatureType buildingSchema() {
		SimpleFeatureType sft = null;
		StringBuilder attributes = new StringBuilder();
		attributes.append("type" + ":String,");
		attributes.append("OBJECTID" + ":String,");
		attributes.append("TOPELEV_M" + ":String,");
		attributes.append("BASEELEV_M" + ":String,");
		attributes.append("HGT_AGL" + ":String,");
		attributes.append("MED_SLOPE" + ":String,");
		attributes.append("ROOFTYPE" + ":Integer,");
		attributes.append("AVGHT_M" + ":String,");
		attributes.append("BASE_M" + ":String,");
		attributes.append("ORIENT8" + ":String,");
		attributes.append("LEN" + ":String,");
		attributes.append("WID" + ":String,");
		attributes.append("GlobalID" + ":String,");
		attributes.append("Shape__Area" + ":String,");
		attributes.append("Shape__Length" + ":String,");
		attributes.append("*" + "geometry" + ":Polygon:srid=4326");
		sft = SimpleFeatureTypes.createType("buildings", attributes.toString());
		// Only enabling xz2 indexing for non-point indexing
		sft.getUserData().put("geomesa.indices.enabled", "xz2");
		return sft;
	}

	public static SimpleFeatureType schoolSchema() {
		SimpleFeatureType sft = null;
		StringBuilder attributes = new StringBuilder();
		attributes.append("type" + ":String,");
		attributes.append("OBJECTID" + ":String,");
		attributes.append("Name" + ":String,");
		attributes.append("description" + ":String,");
		attributes.append("*" + "geometry" + ":Point:srid=4326");
		sft = SimpleFeatureTypes.createType("schools", attributes.toString());
		// Only enabling z2 indexing
		sft.getUserData().put("geomesa.indices.enabled", "z2");
		return sft;
	}

	public static DataStore createDatastore(String datastore) {
		DataStore ds = null;
		if (datastore.equals("cassandra")) {
			ds = cassandraDatastore();
		} else if (datastore.equals("hbase")) {

		} else {
			System.out.print("Input not accepted.");
		}
		return ds;
	}

	public static DataStore cassandraDatastore() {
		DataStore datastore = null;

		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("cassandra.contact.point", "127.0.0.1:9042");
		parameters.put("cassandra.keyspace", "grafittidb");
		parameters.put("cassandra.catalog", "geoycsb");
		/*
		 * for(Entry<String, String> entry: parameters.entrySet()) {
		 * System.out.println("Key=" + entry.getKey() + "   value=" + entry.getValue());
		 * }
		 */
		try {
			datastore = DataStoreFinder.getDataStore(parameters);
			if (datastore == null) {
				throw new RuntimeException("Cannot create datastore given the parameter");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return datastore;
	}

	public static void hbaseDatastore() {

	}

	public static void insertData(DataStore datastore, SimpleFeatureType sft, ArrayList<SimpleFeature> features) {
		if (features.size() > 0) {
			System.out.println("Writing test data");
			// use try-with-resources to ensure the writer is closed
			try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore
					.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
				for (SimpleFeature feature : features) {
					// using a geotools writer, you have to get a feature, modify it, then commit it
					// appending writers will always return 'false' for haveNext, so we don't need
					// to bother checking
					SimpleFeature toWrite = writer.next();

					// copy attributes
					toWrite.setAttributes(feature.getAttributes());

					// if you want to set the feature ID, you have to cast to an implementation
					// class
					// and add the USE_PROVIDED_FID hint to the user data
					// ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
					// toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

					// alternatively, you can use the PROVIDED_FID hint directly
					toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

					// if no feature ID is set, a UUID will be generated for you

					// make sure to copy the user data, if there is any
					toWrite.getUserData().putAll(feature.getUserData());

					// write the feature
					writer.write();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Wrote " + features.size() + " features");
			System.out.println();
		}
		return;
	}

	public static void near1(DataStore datastore, String table) {
		try {
			long start = System.nanoTime();
			SimpleFeatureSource s = datastore.getFeatureSource(table);
			//SimpleFeatureCollection data = s.getFeatures();
			KNearestNeighborSearchProcess process = new KNearestNeighborSearchProcess();
			SimpleFeatureCollection input = DataUtilities.collection(DataUtilities.createFeature(incidentSchema(), 
					"null|null|null|null|null|null|null|null|null|null|null|Point(-111.94157702764069 33.4300795967036)"));

			//			SimpleFeatureBuilder builder = new SimpleFeatureBuilder(incidentSchema());
//			builder.set("OBJECTID", "1");
//			builder.set("geometry", "Point(-111.94157702764069 33.4300795967036)");
//			SimpleFeature feature = builder.buildFeature("1");
//			input = DataUtilities.collection(feature);
			
			SimpleFeatureCollection data = null;
			try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(new Query(table),
					Transaction.AUTO_COMMIT)) {
				data = DataUtilities.collection(reader);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("ERROR");
			};
			
			System.out.println("start");
			SimpleFeatureCollection results = process.execute(input, data, 5, 0.0, 100000.0);
			System.out.println("end");
			System.out.println((System.nanoTime()-start)/1000000L + "ms");
			SimpleFeatureIterator iterator = results.features();
		
//				while (iterator.hasNext()) {
//					SimpleFeature f = iterator.next();
//					System.out.println(DataUtilities.encodeFeature(f));
//
//				} 
			long end = System.nanoTime();
			System.out.println((end-start));
				iterator.close();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}
	public static void near(DataStore datastore, String table) {
		try {
			long start = System.nanoTime();
			SimpleFeatureSource s = datastore.getFeatureSource(table);
			
// Retrieving features using SimpleDataSource
			SimpleFeatureCollection data = s.getFeatures(new Query(table));  
			
//Retrieving using FeatureReader
//			SimpleFeatureCollection data = null;
//			try(FeatureReader<SimpleFeatureType, SimpleFeature> reader = datastore.getFeatureReader(new Query(table),
//					Transaction.AUTO_COMMIT)){
//				data = DataUtilities.collection(reader);
//			}
			
			KNearestNeighborSearchProcess process = new KNearestNeighborSearchProcess();
			SimpleFeatureBuilder builder = new SimpleFeatureBuilder(incidentSchema());
			builder.set("OBJECTID", "1");
			builder.set("geometry", "Point(-111.94157702764069 33.4300795967036)");
			SimpleFeature feature = builder.buildFeature("1");
			SimpleFeatureCollection input = DataUtilities.collection(feature);
			System.out.println("start");
			SimpleFeatureCollection results = process.execute(input, data, 5, 0.0, 100000.0);
			System.out.println("end");
			System.out.println((System.nanoTime()-start)/1000000L + "ms");
			SimpleFeatureIterator iterator = results.features();
		
//				while (iterator.hasNext()) {
//					SimpleFeature f = iterator.next();
//					System.out.println(DataUtilities.encodeFeature(f));
//
//				} 
			long end = System.nanoTime();
			System.out.println((end-start));
				iterator.close();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}
	
	
	public static long runQueryReader(DataStore ds, String table, String filter) {
		try {
			 //create query
			Query q = new Query(table, ECQL.toFilter(filter));
			long start = System.nanoTime();
			// submit query
			FeatureReader<SimpleFeatureType, SimpleFeature> reader = ds.getFeatureReader(q,Transaction.AUTO_COMMIT);
			long timeElapse = System.nanoTime() - start;	//stop timer
			
			System.out.println(String.format("		Time(Reader): %s ms", Long.toString(timeElapse/1000000L))); //convert into ms
			while (reader.hasNext()) {
				System.out.println(reader.next().getID());
			}
			reader.close();

			return  timeElapse/1000000L;
		} catch (Exception e1) {
		e1.printStackTrace();
		}
		return -1L;
	}
	
	public static long runQuerySource(DataStore ds, String table, String filter) {
		try {
			long start = System.nanoTime();
			SimpleFeatureSource s = ds.getFeatureSource(table);
			// submit query
			SimpleFeatureCollection c = s.getFeatures(ECQL.toFilter(filter));
			
			SimpleFeatureIterator reader = c.features();
			long timeElapse = System.nanoTime() - start; //stop timer
			System.out.println(String.format("Time(Source): %s ms", Long.toString(timeElapse/1000000L))); //convert into ms
			while (reader.hasNext()) {
				System.out.println(reader.next().getID());
			}
			reader.close();
			return  timeElapse/1000000L;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return -1L;
	}
	
	/**
	 * 
	 * @param args arg[0] = datastore name arg[1] = table name arg[2] = file path
	 */
	public static void main(String[] args) {
		String resources = System.getProperty("user.dir") + "/src/main/resources/";
		String[] incidents = {"incidents",resources+"Graffiti_Abatement_IncidentsLine.json"};
		String[] buildings = {"buildings", resources+"Tempe_Buildings.geojson"};
		String[] schools = {"schools", resources+"K-12_Tempe_Schools.json"};
		ArrayList<String[]> list = new ArrayList<>();
		list.add(incidents);
		list.add(buildings);
		list.add(schools);
		DataStore ds = createDatastore("cassandra"); // datastore
		
		
		for(String[] param: list) {
			SimpleFeatureType sft = createSchema(param[0]); // tablename
			try {
				ds.createSchema(sft);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			insertData(ds, sft, generateFeatures(sft, param[1]));
		}
		SimpleFeatureType sft = createSchema(args[1]); // tablename
		try {
			ds.createSchema(sft);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		insertData(ds, sft, generateFeatures(sft, args[2]));

		
//		String filter = "INTERSECTS(incidents, MultiLineString(( -111.91662294557443 33.3964260528521, -111.81731649209647 33.57158772492773), (-111.53584670300357 33.88361677560589, -111.0710866287766 33.47761102293026)))";
//		double initial = -111.8802089256232;
//		
//		String table = "incidents";
//		int iteration = 11;
//		long readerTime = 0; //in ms
//		long sourceTime = 0; //in ms
//		for(int i = 0; i < iteration; i++) {
//			if(i == 0) {
//
//				//runQuerySource(ds, table, filter);
//				//runQueryReader(ds, table, filter);
//
//				runQuerySource(ds, table, String.format(filter, Double.toString(initial)));
//				runQueryReader(ds, table, String.format(filter, Double.toString(initial)));
//			}
//			else {
//				//sourceTime += runQuerySource(ds, table, filter);
//				//readerTime += runQueryReader(ds, table, filter);
//				
//				//sourceTime += runQuerySource(ds, table, String.format(filter, Double.toString(initial+i*0.0001)));
//				//readerTime += runQueryReader(ds, table, String.format(filter, Double.toString(initial+i*0.0001)));
//				
//				sourceTime += runQuerySource(ds, table, String.format(filter, Double.toString(initial)));
//				readerTime += runQueryReader(ds, table, String.format(filter, Double.toString(initial)));
//			}
//		}
//		
//		System.out.println(String.format("avg runtime reader: %s ms, avg runtime source: %s ms",
//				Long.toString(readerTime/10L), Long.toString(sourceTime/10L)));
//		
//		//near(ds, "incidents");
		return;

	}
}
