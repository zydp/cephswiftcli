package rgwswiftcli

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type bucket struct {
	buckets map[string]bool
	handle  *RgwSwiftCli
}

/*------------------------------------------------------------------------------*/
/*
 * "a list of user containers"
 */
func (this bucket) List() (buckets []string, err error) {

	request, err := http.NewRequest("GET", this.handle.storageUrl, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)

	httpData, err := this.handle.httpHandle.Do(request)
	if err != nil {
		return
	}

	buf, len, err := this.handle.dataRead(httpData.Body)
	if len < 1 {
		err = errors.New("User Bucket list is empty")
	}
	buckets = strings.Split(string(buf), "\n")
	return buckets, err
}

/*------------------------------------------------------------------------------*/
/*
 * "To create a new bucket(container)
 * 1、bucketID： bucket name. Required
 * 2、readUser: The user IDs with read permissions for the container. optional
 * 3、writeUser: The user IDs with write permissions for the container. optional"
 */
func (this bucket) Create(bucketID string) (err error) {
	return this.CreateWithAcl(bucketID, nil, nil)
}

func (this bucket) CreateWithAcl(bucketID string, readUser, writeUser []string) (err error) {
	url := fmt.Sprintf("%s/%s", this.handle.storageUrl, bucketID)
	request, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	var bufWrite bytes.Buffer
	if len(readUser) > 0 {
		for i, tmp := range readUser {
			if i > 0 {
				bufWrite.WriteByte(',')
			}
			bufWrite.WriteString(tmp)
		}
		request.Header.Set("X-Container-Read", bufWrite.String())
	}
	bufWrite.Reset()
	if len(writeUser) > 0 {
		var bufWrite bytes.Buffer
		for i, tmp := range readUser {
			if i > 0 {
				bufWrite.WriteByte(',')
			}
			bufWrite.WriteString(tmp)
		}
		request.Header.Set("X-Container-Write", bufWrite.String())
	}
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
 * "To allow other users to read a buckets’s contents or write to a buckets
 * bucketID: bucket name
 * readUser: The user IDs with read permissions for the container, * is all user
 * writeUser: The user IDs with write permissions for the container. * is all user"
 */
func (this bucket) UpdateAcls(bucketID string, readUser, writeUser []string) (err error) {
	url := fmt.Sprintf("%s/%s", this.handle.storageUrl, bucketID)
	request, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return
	}
	request.Header.Set("X-Auth-Token", this.handle.userToken)
	var bufWrite bytes.Buffer
	if len(readUser) > 0 {
		for i, tmp := range readUser {
			if i > 0 {
				bufWrite.WriteByte(',')
			}
			bufWrite.WriteString(tmp)
		}
		request.Header.Set("X-Container-Read", bufWrite.String())
	}
	bufWrite.Reset()
	if len(writeUser) > 0 {
		var bufWrite bytes.Buffer
		for i, tmp := range readUser {
			if i > 0 {
				bufWrite.WriteByte(',')
			}
			bufWrite.WriteString(tmp)
		}
		request.Header.Set("X-Container-Write", bufWrite.String())
	}
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
 * "remove a bucket"
 */
func (this bucket) Remove(bucketID string) (err error) {
	url := fmt.Sprintf("%s/%s", this.handle.storageUrl, bucketID)
	request, err := http.NewRequest("DELETE", url, nil)
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
/* "list a bucket's objects
 * prefix: Limits the result set to objects beginning with the specified prefix. optional
 * marker: Returns a list of results greater than the marker value. optional
 * limit: Limits the number of results to the specified value.Range:0-10,000. optional
 * path: The pseudo-hierarchical path of the objects. optional"
 */
func (this bucket) Objects(bucketID string) (keys []string, err error) {
	return this.ObjectsRaw(bucketID, "", "", "", 1000)
}

func (this bucket) ObjectsByLimit(bucketID string, limit uint32) (keys []string, err error) {
	return this.ObjectsRaw(bucketID, "", "", "", limit)
}

func (this bucket) ObjectsByPrefix(bucketID, prefix string, limit uint32) (keys []string, err error) {
	return this.ObjectsRaw(bucketID, prefix, "", "", limit)
}

func (this bucket) ObjectsRaw(bucketID, prefix, marker, path string, limit uint32) (keys []string, err error) {
	url := fmt.Sprintf("%s/%s", this.handle.storageUrl, bucketID)
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
	if l < 1 {
		err = errors.New("User Object list is empty")
	}
	keys = strings.Split(string(resp), "\n")
	return
}
