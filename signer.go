package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

var md5Mutex sync.Mutex
var ch sync.Mutex

func SingleHash(in, out chan interface{}) {
	start := time.Now()
	defer func() {
		fmt.Println("SingleHash lasted for", time.Since(start))
	}()
	wg := sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			valueChan := make(chan string)
			valueChan2 := make(chan string)
			dataString := strconv.Itoa(data.(int))
			go func() {
				valueChan <- DataSignerCrc32(dataString)
			}()
			md5Mutex.Lock()
			md5 := DataSignerMd5(dataString)
			md5Mutex.Unlock()
			go func() {
				valueChan2 <- DataSignerCrc32(md5)
			}()
			crc := <-valueChan
			crc2 := <-valueChan2
			out <- crc + "~" + crc2
			wg.Done()
		}(data)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	start := time.Now()
	defer func() {
		fmt.Println("MultiHash lasted for", time.Since(start))
	}()
	wgCommon := &sync.WaitGroup{}
	for data := range in {
		wgCommon.Add(1)
		go func(data interface{}) {
			wg := sync.WaitGroup{}
			result := make([]string, 6)
			dataString := data.(string)
			wg.Add(6)
			for i := 0; i < 6; i++ {
				go func(i int) {
					result[i] = DataSignerCrc32(strconv.Itoa(i) + dataString)
					wg.Done()
				}(i)
			}
			wg.Wait()
			var res string
			for i := 0; i < 6; i++ {
				res = res + result[i]
			}
			wgCommon.Done()
			ch.Lock()
			out <- res
			ch.Unlock()
		}(data)
	}
	wgCommon.Wait()
}

func CombineResults(in, out chan interface{}) {
	start := time.Now()
	defer func() {
		fmt.Println("CombineResult lasted for", time.Since(start))
	}()
	var results []string
	for data := range in {
		results = append(results, data.(string))
	}
	sort.Strings(results)
	var result string
	for i, str := range results {
		if i > 0 {
			result = result + "_"
		}
		result = result + str
	}
	out <- result
}

func ExecutePipeline(jobs ...job) {
	start := time.Now()
	defer func() {
		fmt.Println("ExecutePipeline lasted for", time.Since(start))
	}()
	inputChan := make(chan interface{})
	wg := sync.WaitGroup{}
	wg.Add(len(jobs))
	for _, singleJob := range jobs {
		outputChan := make(chan interface{})
		go func(inputChan, outputChan chan interface{}, singleJob job) {
			defer func() {
				ch.Lock()
				close(outputChan)
				ch.Unlock()
			}()
			singleJob(inputChan, outputChan)
			wg.Done()
		}(inputChan, outputChan, singleJob)
		inputChan = outputChan
	}
	wg.Wait()
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Println(time.Since(start))
	}()
	inputChan := make(chan interface{})
	outputChan := make(chan interface{})
	defer close(outputChan)
	go SingleHash(inputChan, outputChan)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		fmt.Println(<-outputChan)
		wg.Done()
	}()
	inputChan <- 0
	wg.Wait()
	close(inputChan)
}
