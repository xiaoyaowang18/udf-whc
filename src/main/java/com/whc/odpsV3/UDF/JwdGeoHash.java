package com.whc.odpsV3;

import ch.hsr.geohash.GeoHash;
import com.aliyun.odps.udf.UDF;


public class JwdGeoHash extends UDF {
    public String evaluate(String jd, String wd) throws Exception {
        Double JD_DB = Double.parseDouble(jd);
        Double WD_DB = Double.parseDouble(wd);
        GeoHash geoHash = GeoHash.withBitPrecision(WD_DB, JD_DB, 60);
        String userGeohash = geoHash.toBase32();
        return userGeohash;
    }

    public static void main(String[] args) throws Exception {
        JwdGeoHash jwdGeoHash = new JwdGeoHash();
        String evaluate = jwdGeoHash.evaluate("110.431932", "23.41844");
        System.out.println(evaluate);
    }
}