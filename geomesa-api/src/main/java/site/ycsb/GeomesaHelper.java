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
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;

public class GeomesaHelper {
	public static ArrayList<SimpleFeature> generateFeatures(SimpleFeatureType sft, String file) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		ArrayList<SimpleFeature> f = new ArrayList<>();
		String table = sft.getTypeName();
		try {
			Scanner reader = new Scanner(new File(file));
			while(reader.hasNext()) {
				JSONObject json = new JSONObject(reader.nextLine());
				if(table.equals("incidents")) {
					f.add(incidentData(builder, json));
				}
				//System.out.println(json);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return f;
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
		if(type.equals("Point")) {
			spatial = String.format("Point(%s %s)", getX(coordinates), getY(coordinates));
		}
		if(type.equals("MultiLineString")) {
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
	
	private static String getX(JSONArray coordinates) {
		//return coordinates.getBigDecimal(0) + "";
		return coordinates.getDouble(0) + ""; // x value
	}
	
	private static String getY(JSONArray coordinates) {
		//return coordinates.getBigDecimal(1) + "";
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

			f = builder.buildFeature(properties.get("OBJECTID")+"");
		}catch(Exception e) {
			System.err.println("Error when converting data into SimpleFeature.");
		}
		return f;
	}
	
	public static SimpleFeatureType createSchema(String table) {
		SimpleFeatureType sft = null;
		if(table.equals("incidents")) {
			
			sft = incidentSchema();
		}
		else {
			
		}
		if (sft == null) throw new RuntimeException("Error encounter when creating schema for " + table);
		
		return sft;
	}
	
	public static SimpleFeatureType incidentSchema() {
		SimpleFeatureType sft = null;
		StringBuilder attributes = new StringBuilder();
		attributes.append("type" + ":String,");
		attributes.append("OBJECTID" + ":String,");
		attributes.append("INCIDENT_NUMBER" + ":String,");
		//Modify LOCATION -> _LOCATION due to geomesa reserved word list
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
		//Only enabling z2 indexing
		sft.getUserData().put("geomesa.indices.enabled", "z2");
		return sft;
	}
	
	public static DataStore createDatastore(String datastore) {
		DataStore ds = null;
		if (datastore.equals("cassandra")){
				ds = cassandraDatastore();
		}else if (datastore.equals("hbase")) {
			
		}else {
				System.out.print("Input not accepted.");
		}
		return ds;
	}
	
	public static DataStore cassandraDatastore() {
		DataStore datastore = null;
		/*
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please enter catalog table name: ");
		String catalog = scanner.nextLine();
		System.out.println("Please enter contact point information(ex. 127.0.0.1:9042):");
		String contactpoint = scanner.nextLine();
		System.out.println("Please enter keyspace name: ");
		String keyspace = scanner.nextLine();
		
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("cassandra.contact.point", contactpoint);
		parameters.put("cassandra.keyspace", keyspace);
		parameters.put("cassandra.catalog", catalog);
		*/
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("cassandra.contact.point", "127.0.0.1:9042");
		parameters.put("cassandra.keyspace", "testdb");
		parameters.put("cassandra.catalog", "incidents");
		/*
		for(Entry<String, String> entry: parameters.entrySet()) {
            System.out.println("Key=" + entry.getKey() + "   value=" + entry.getValue());
        }
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
            try ( 
                FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                     //((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
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
	/**
	 * 
	 * @param args 
	 * arg[0] = datastore name
	 * arg[1] = table name
	 * arg[2] = file path
	 */
	public static void main(String[] args) {
		
		DataStore ds = createDatastore(args[0]); //datastore
		SimpleFeatureType sft = createSchema(args[1]); //tablename
		try {
			ds.createSchema(sft);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		insertData(ds, sft, generateFeatures(sft, args[2]));
		
	
		return;
		
	}
}
