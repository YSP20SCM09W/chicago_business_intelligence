package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
// The following program will collect data for Taxi Trips, Building permists, and
// Unemployment data from the City of Chicago data portal
// we are using SODA REST API to collect the JSON records
// You coud use the REST API below and post them as URLs in your Browser
// for manual inspection/visualization of data
// the browser will take roughly 5 minutes to get the reply with the JSON data
// and product the pretty-print
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// trip_id	"c354c843908537bbf90997917b714f1c63723785"
// trip_start_timestamp	"2021-11-13T22:45:00.000"
// trip_end_timestamp	"2021-11-13T23:00:00.000"
// trip_seconds	"703"
// trip_miles	"6.83"
// pickup_census_tract	"17031840300"
// dropoff_census_tract	"17031081800"
// pickup_community_area	"59"
// dropoff_community_area	"8"
// fare	"27.5"
// tip	"0"
// additional_charges	"1.02"
// trip_total	"28.52"
// shared_trip_authorized	false
// trips_pooled	"1"
// pickup_centroid_latitude	"41.8335178865"
// pickup_centroid_longitude	"-87.6813558293"
// pickup_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6813558293
// 		1	41.8335178865
// dropoff_centroid_latitude	"41.8932163595"
// dropoff_centroid_longitude	"-87.6378442095"
// dropoff_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6378442095
// 		1	41.8932163595
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}
type BuidngPermitJsonRecords []struct {
	Permit_        string `json:"permit_"`
	Permit_type    string `json:"permit_type"`
	Reported_cost  string `json:"reported_cost"`
	Community_area string `json:"community_area"`
	Latitude       string `json:"latitude"`
	Longitude      string `json:"longitude"`
	Zip_code       string `json:"zip_code"`
}
type unemploymentRatesJsonRecords []struct {
	Community_area_name    string `json:"community_area_name"`
	Birth_rate             string `json:"birth_rate"`
	General_fertility_rate string `json:"general_fertility_rate"`
	Below_poverty_level    string `json:"below_poverty_level"`
	Unemployment           string `json:"unemployment"`
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=localhost sslmode=disable port = 5432"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=172.19.0.2 sslmode=disable port = 5433"

	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/tidal-skill-348904:us-central1:mypostgres sslmode=disable port = 5432"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))

	// Test the database connection
	//err = db.Ping()
	//if err != nil {
	//	fmt.Println("Couldn't Connect to database")
	//	panic(err)
	//}

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary
	for {
		// build and fine-tune functions to pull data from different data sources
		// This is a code snippet to show you how to pull data from different data sources//.
		GetTaxiTrips(db)
		GetUnemploymentRates(db)
		GetBuildingPermits(db)

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTaxiTrips(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2
	// write your own Api-key
	geocoder.ApiKey = "AIzaSyAOaFoDewsUlPw8PlRiZhPwudSIazRBXWs"

	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("Collecting Building Permits Data")
	// write your own Api-key
	geocoder.ApiKey = "AIzaSyAOaFoDewsUlPw8PlRiZhPwudSIazRBXWs"
	fmt.Println("Starting to create table:building_permit")
	drop_table := `drop table if exists building_permit`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permit" (
                        "id"   SERIAL , 
                        "permit_" VARCHAR(255), 
						"reported_cost" DOUBLE PRECISION, 
                        "permit_type" VARCHAR(255), 
                        "community_area" INTEGER, 
                        "longitude" DOUBLE PRECISION, 
						"latitude" DOUBLE PRECISION, 
                        "zip_code" VARCHAR(255),  
                        PRIMARY KEY ("id") 
                    );`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("get data from soda api")
	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=5000"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permit_list BuidngPermitJsonRecords
	json.Unmarshal(body, &building_permit_list)
	fmt.Println("Write data to database table")
	for i := 0; i < len(building_permit_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		permit_ := building_permit_list[i].Permit_
		if permit_ == "" {
			continue
		}

		permit_type := building_permit_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		if permit_type != "PERMIT - NEW CONSTRUCTION" {
			continue
		}

		reported_cost := building_permit_list[i].Reported_cost
		if reported_cost == "" {
			continue
		}

		community_area := building_permit_list[i].Community_area
		if community_area == "" {
			continue
		}

		latitude := building_permit_list[i].Latitude
		if latitude == "" {
			continue
		}
		longitude := building_permit_list[i].Longitude
		if longitude == "" {
			continue
		}

		latitude_float, _ := strconv.ParseFloat(latitude, 64)
		longitude_float, _ := strconv.ParseFloat(longitude, 64)

		bld_location := geocoder.Location{
			Latitude:  latitude_float,
			Longitude: longitude_float,
		}

		address_list, _ := geocoder.GeocodingReverse(bld_location)
		address := address_list[0]
		zip_code := address.PostalCode

		sql := `INSERT INTO building_permit ("permit_", "permit_type", "reported_cost", "community_area", "latitude", "longitude", "zip_code") values($1, $2, $3, $4, $5, $6, $7)`

		_, err = db.Exec(
			sql,
			permit_,
			permit_type,
			reported_cost,
			community_area,
			latitude,
			longitude,
			zip_code)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("table has been implemented")
}

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")
	fmt.Println("Starting to create table:unploymentrates")
	drop_table := `drop table if exists unploymentrates`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unploymentrates" (
		"id"   SERIAL , 
		"community_area_name" VARCHAR(255), 
		"birth_rate" DOUBLE PRECISION, 
		"general_fertility_rate" DOUBLE PRECISION, 
		"below_poverty_level" DOUBLE PRECISION, 
		"unemployment" DOUBLE PRECISION,  
		PRIMARY KEY ("id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("get data from soda api")
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=10000"
	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var unemploymentRates_list unemploymentRatesJsonRecords
	json.Unmarshal(body, &unemploymentRates_list)
	fmt.Println("Write data to database table")
	for i := 0; i < len(unemploymentRates_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		// If no permit data or permit type is not new construction then skip it
		community_area_name := unemploymentRates_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		birth_rate := unemploymentRates_list[i].Birth_rate
		if birth_rate == "" {
			continue
		}

		general_fertility_rate := unemploymentRates_list[i].General_fertility_rate

		below_poverty_level := unemploymentRates_list[i].Below_poverty_level

		unemployment := unemploymentRates_list[i].Unemployment
		if unemployment == "" {
			continue
		}

		sql := `INSERT INTO unploymentrates ("community_area_name", "birth_rate", "general_fertility_rate", "below_poverty_level", "unemployment") values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			community_area_name,
			birth_rate,
			general_fertility_rate,
			below_poverty_level,
			unemployment)

		if err != nil {
			panic(err)
		}
	}
	fmt.Println("GetUnemploymentRates: Implement Unemployment")
}
