package rgwswiftcli

import (
	"fmt"
	"testing"
)

const (
	serverAddress = "https://192.168.10.42"
	certPath      = "selfsigned.pem"
	user          = "via:swift"
	key           = "yo4K9g8Nf5BpsIIWRb08B4AuxbK9I060qYTqyrpz"
)

func TestSwiftClient(t *testing.T) {
	fmt.Printf("%s\n", "Hi, SwiftCli")

	/*create a handle*/
	var SwiftHandle = &RgwSwiftCli{ServerAddress: serverAddress, PathOfCert: certPath}

	/*user auth*/
	if err := SwiftHandle.Auth(user, key); nil != err {
		fmt.Println("User Auth Failed:", err.Error())
	}

	/*bucket create*/
	if err := SwiftHandle.Bucket.Create("via-bucket001"); nil != err {
		fmt.Println("Create Bucket Failed:", err.Error())
	}

	/*bucket create with acl*/
	var reader = []string{"ping:swift"}
	if err := SwiftHandle.Bucket.CreateWithAcl("via-bucket002", reader, nil); nil != err {
		fmt.Println("Create Bucket Failed:", err.Error())
	}

	/*create objects*/
	if err := SwiftHandle.Object.Create("via-bucket001", "AAA", "123456789abcdefghij"); nil != err {
		fmt.Println("Create Object Failed:", err.Error())
	}
	if err := SwiftHandle.Object.CreateByFile("via-bucket001", "TheSwordsman", "./swordsman.png"); nil != err {
		fmt.Println("Create Object By A File Failed:", err.Error())
	}

	/*list objects by a bucket*/
	if keys, err := SwiftHandle.Bucket.ObjectsByLimit("via-bucket001", 10); nil != err {
		fmt.Println("List Objects By A Bucket Failed:", err.Error())
	} else {
		fmt.Println("Object keys:", keys)
	}

	/*get an object*/
	if val, err := SwiftHandle.Object.Get("via-bucket001", "AAA"); nil != err {
		fmt.Println("Get An Object Failed:", err.Error())
	} else {
		fmt.Println("AAA Value is:", val)
	}

	/*get an object by range*/
	if val, err := SwiftHandle.Object.GetByRange("via-bucket001", "AAA", 0, 5); nil != err {
		fmt.Println("Get An Object By Range Failed:", err.Error())
	} else {
		fmt.Println("AAA Value 0-5 is:", val)
	}

	/*get an object and save to a file*/
	if err := SwiftHandle.Object.GetToFile("via-bucket001", "TheSwordsman", "./TheSwordsmanCOPY.png"); nil != err {
		fmt.Println("Get An Object And Save It To A File Failed:", err.Error())
	}

	/*copy an object*/
	if err := SwiftHandle.Object.Copy("via-bucket001", "AAA", "via-bucket002", "AAA-COPY"); nil != err {
		fmt.Println("Copy An Object Failed:", err.Error())
	}

	/*list objects by a bucket*/
	if keys, err := SwiftHandle.Bucket.ObjectsByLimit("via-bucket001", 10); nil != err {
		fmt.Println("List Objects By A Bucket Failed:", err.Error())
	} else {
		fmt.Println("'via-bucket001' Object keys:", keys)
	}

	/*list objects by a bucket*/
	if keys, err := SwiftHandle.Bucket.ObjectsByLimit("via-bucket002", 10); nil != err {
		fmt.Println("List Objects By A Bucket Failed:", err.Error())
	} else {
		fmt.Println("'via-bucket002' Object keys:", keys)
	}

	/*remove object*/
	SwiftHandle.Object.Remove("via-bucket001", "AAA")
	SwiftHandle.Object.Remove("via-bucket001", "TheSwordsman")
	SwiftHandle.Object.Remove("via-bucket002", "AAA-COPY")

	/*remove bucket*/
	SwiftHandle.Bucket.Remove("via-bucket001")
	SwiftHandle.Bucket.Remove("via-bucket002")
}
