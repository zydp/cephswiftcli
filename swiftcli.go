package rgwswiftcli

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

/*------------------------------------------------------------------------------*/
/*
 * 'Swift Api Client for Rados Gateway
 * ServerAddress : "http(s)//x.x.x.x"
 * PathOfCert ："selfsigned.pem"'
 */
type RgwSwiftCli struct {
	/* Public */
	ServerAddress string
	PathOfCert    string
	/* Private */
	user          string
	userSecretKey string
	userToken     string
	url           *url.URL
	storageUrl    string
	httpHandle    *http.Client
	Bucket        bucket
	Object        object
}

/*------------------------------------------------------------------------------*/
/*
 * "user auth response"
 */
type authResp struct {
	Code      string
	HostId    string
	RequestId string
}

/*------------------------------------------------------------------------------*/
/*
 * "swift user authentication"
 */
func (this *RgwSwiftCli) Auth(UserName, SecretKey string) (err error) {
	this.url, err = url.Parse(this.ServerAddress)
	if err != nil {
		return err
	}
	var trans *http.Transport = nil
	if this.PathOfCert != "" {
		caCert, err := ioutil.ReadFile(this.PathOfCert)
		if err != nil {
			return err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		trans = &http.Transport{
			TLSClientConfig: &tls.Config{
				// Certificates: []tls.Certificate{cert}, //proviede by the client ???
				RootCAs: caCertPool,
				//ServerName: "domain",
			},
		}
	}
	this.httpHandle = &http.Client{
		Transport: trans,
		Timeout:   5 * time.Second,
	}
	request, err := http.NewRequest("GET", this.url.String()+"/auth", nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Auth-User", UserName)
	request.Header.Set("X-Auth-Key", SecretKey)
	httpData, err := this.httpHandle.Do(request)
	if err != nil {
		return err
	}
	if httpData.StatusCode != 204 {
		var resp authResp
		length := 0
		var buffer = make([]byte, 1024)
		r := httpData.Body
		for {
			n, err := r.Read(buffer)
			length = length + n
			if err == io.EOF || nil != err {
				break
			}
			if length > 1024 {
				break
			}
		}
		if err = json.Unmarshal(buffer[:length], &resp); nil != err {
			return err
		}
		return errors.New(resp.Code)
	}
	this.userToken = httpData.Header.Get("x-auth-token")
	this.storageUrl = httpData.Header.Get("X-Storage-Url")
	this.Bucket.handle = this
	this.Object.handle = this
	return nil
}

/*------------------------------------------------------------------------------*/
/*
 * "read data from reader, return a byte array"
 */
func (this *RgwSwiftCli) dataRead(h io.Reader) (rbuffer []byte, n int, err error) {
	var tmp = make([]byte, 4096)
	var buf bytes.Buffer
	for {
		n, err := h.Read(tmp)
		if n > 0 {
			buf.Write(tmp[0:n])
		}
		if err == io.EOF || nil != err {
			break
		}
	}
	return buf.Bytes(), buf.Len(), err
}
