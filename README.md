# CSYE7200Spring2020Project

We are developing an application that will persist Bitcoin trading data and use it further for analysis and forecasting.

We will make a REST API call to fetch transactions from a cryptocurrency exchange. A cryptocurrency exchange allows customers to trade digital currencies, such as bitcoin, for fiat currencies, such as the US dollar. The transaction data will allow us to track the price and quantity exchanged at a certain point in time. In our case the cryptocurrency exchange platform will be BitStamp. (https://www.bitstamp.net/api/)

We will then introduce the Parquet format. This is a columnar data format that is widely used for big data analytics. After that, we will build a standalone application that will produce a history of bitcoin/USD transactions and save it in Parquet. 

Further we will also use live trading data using streaming from WebSocket API (https://www.bitstamp.net/websocket/v2/). After subscribing to this live channel, we will use Kafka Topics for the temporary storage of the streamed data.

Then, we will use Apache Zeppelin to query and analyze the data interactively.

### Tools & Technologies used in the project

<br>Scala</br>
<br>Apache Spark</br>
<br>Parquet format</br>
<br>Zookeeper</br>
<br>Kafka Topics</br>
<br>Zeppelin</br>
<br>SparkML</br>





## License
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
