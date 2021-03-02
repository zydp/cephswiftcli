package rgwswiftcli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type object struct {
	handle *RgwSwiftCli
}

/*------------------------------------------------------------------------------*/
/*
 * "To create a new object"
 */
func (this object) Create(bucket, key, value string) (err error) {
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("PUT", url, strings.NewReader(value))
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	resp, l, err := this.handle.dataRead(httpData.Body)
	if l > 0 {
		return errors.New(string(resp))
	}
	return err
}

/*------------------------------------------------------------------------------*/
/*
 * "To create a new object, and set the file as the value"
 */
func (this object) CreateByFile(bucket, key, filePath string) (err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("PUT", url, bufio.NewReader(f))
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	resp, l, err := this.handle.dataRead(httpData.Body)
	if l > 0 {
		return errors.New(string(resp))
	}
	return err
}

/*------------------------------------------------------------------------------*/
/*
 * "To retrieve an object"
 */
func (this object) Get(bucket, key string) (resp []byte, err error) {
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	resp, l, err := this.handle.dataRead(httpData.Body)
	if l > 0 {
		return resp, errors.New(string(resp))
	}
	return resp, err
}

/*------------------------------------------------------------------------------*/
/*
 * "To retrieve an object, and save it to a file"
 */
func (this object) GetToFile(bucket, key, filePath string) (err error) {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	io.Copy(bufio.NewWriter(f), httpData.Body)
	return
}

/*------------------------------------------------------------------------------*/
/*
 * "To retrieve a subset of an object’s contents"
 */
func (this object) GetByRange(bucket, key string, start, end uint32) (resp []byte, err error) {
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	request.Header.Set("range", fmt.Sprintf("bytes=%d-%d", start, end))

	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	resp, l, err := this.handle.dataRead(httpData.Body)
	if l < 0 {
		return resp, errors.New(string(resp))
	}
	return
}

/*------------------------------------------------------------------------------*/
/*
 * "Copying an object allows you to make a server-side copy of an object"
 */
func (this object) Copy(bucket_s, key_s, bucket_d, key_d string) (err error) {
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket_d, key_d)
	request, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Copy-From", fmt.Sprintf("%s/%s", bucket_s, key_s))
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}
	resp, l, err := this.handle.dataRead(httpData.Body)
	if l > 0 {
		return errors.New(string(resp))
	}
	return err
}

/*------------------------------------------------------------------------------*/
/*
 * "To delete an object"
 */
func (this object) Remove(bucket, key string) (Eerr error) {
	url := fmt.Sprintf("%s/%s/%s", this.handle.storageUrl, bucket, key)
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	_, err = this.handle.httpHandle.Do(request)
	return
}
