package main

import (
    "io"
    "net"
    "fmt"
    "bufio"
    "strings"
    "time"
    "os"
    "log"
    "net/http"
    "encoding/json"
    "storage"
    "io/ioutil"
    "strconv"
    "sort"

)
var Storages []storage.Message
var Store map[string]string
var StoreChannel chan storage.Message
var Id int

func main() {
    logStandardOutput("Server is starting...")
    //fmt.Println("Server is starting... ")
    file, err := os.Open("databaseLogs.txt")
    if err != nil {
        logError("Error: opening file log.txt failed :: ", err)
        //fmt.Println("Error: opening file log txt failed:::", err) //error
        f, err := os.Create("databaseLogs.txt")
        if err != nil {
            logError("Error: creating file log.txt failed :: ", err)
            return
        }
        defer f.Close()
    } else {
        getExistingData(file)
    }
    defer file.Close()
    errorFile, err := os.Open("errorFile.txt")
    if err != nil {
        logError("Error: opening file errorFile.txt failed :: ", err)
        f, err := os.Create("errorFile.txt")
        if err != nil {
            logError("Error: creating file errorFile.txt failed :: ", err)
            return
        }
        defer f.Close()
    }
    defer errorFile.Close()

    logFile, err := os.Open("allLogs.txt")
    if err != nil {
        logError("Error: opening file allLogs.txt failed :: ", err)
        f, err := os.Create("allLogs.txt")
        if err != nil {
            logError("Error: creating file allLogs.txt failed :: ", err)
            return
        }
        defer f.Close()
    }
    defer logFile.Close()
    Init()

    //logStandardOutput()
    //fmt.Println(Store)
    //go doEvery(1*time.Minute, printMapStore)
    go serveTcp(":5000") //go routine for tcp connections on port 5000
    go handleRequests(":10000") //http
    go updateMainStore(StoreChannel)
    go serveUDP(":5001")
    // keep server alive untill stopped
    for {
        buf := bufio.NewReader(os.Stdin)
        //fmt.Print("> ")
        sentence, err := buf.ReadBytes('\n')
        if err != nil {
            logError("Error::: ", err)
        }
        if strings.TrimSpace(string(sentence)) == "STOP" {
            //close(StoreChannel)
            return
        }
    }
}

func Init() {
    StoreChannel = make(chan storage.Message, 1)
    Store = make(map[string]string)
    file, _ := os.OpenFile("allLogs.txt", os.O_APPEND|os.O_WRONLY, 0644)
    mw := io.MultiWriter(os.Stdout, file)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    log.SetOutput(mw)
}

func doEvery(d time.Duration, f func(t time.Time)) {
    for x := range time.Tick(d) {
        f(x)
    }
}

func printMapStore(t time.Time) {
    fmt.Println("STORE IS AS FOLLOWS @@: ", t.Format("2 Jan 2006 15:04:05"))
    keys := make([]string, 0, len(Store))
    for k := range Store {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _, k := range keys {
        fmt.Println(k, Store[k])
    }
}
//
func getExistingData(file *os.File) {
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        s := strings.Split(line, " ")
        //fmt.Println(line)
        //fmt.Println(s)
        key := s[1]
        value := s[2]
        command := s[3]
        Id, _ = strconv.Atoi(s[0])
        Store[key] = value
        switch command {
        case "POST":
            Store[key] = value
        case "PUT":
            _, ok := Store[key]
            if ok {
                Store[key] = value
            }
        case "DELETE":
            _, ok := Store[key]
            if ok {
                delete(Store, key)
            }
        default:
            fmt.Println("unknown command case: ", command)
        }
    }
}

func updateMainStore(storeChannel chan storage.Message) {
    id := Id+1
    for data := range storeChannel {
        switch data.Command {
        case "POST":
            _, exist := Store[data.Key]
            if exist {
                logStandardOutput("That key already exists: " + data.Key)
            }
            Store[data.Key] = data.Value
            stringedId := strconv.Itoa(id)

            backUp := stringedId + " " + string(data.Key) +" "+ string(data.Value) + " " + data.Command
            f, _ := os.OpenFile("databaseLogs.txt", os.O_APPEND|os.O_WRONLY, 0644)
            fmt.Fprintln(f, backUp) //data entry
            id ++
        case "PUT":
            _, exist := Store[data.Key]
            if exist {
                Store[data.Key] = data.Value
                stringedId := strconv.Itoa(id)
                backUp :=  stringedId+ " " + string(data.Key) + " " +  string(data.Value) + " " + data.Command
                f, _ := os.OpenFile("databaseLogs.txt", os.O_APPEND|os.O_WRONLY, 0644)
                fmt.Fprintln(f, backUp) //data entry
                id ++
            } else {
                logStandardOutput("Couldnt find the key to update main store:" + data.Key)
                //fmt.Println("Couldnt find the key to update main store: " + data.Key) //error?
            }
        case "DELETE":
            _, exist := Store[data.Key]
            if exist {
                delete(Store, data.Key)
                stringedId := strconv.Itoa(id)

                backUp :=  stringedId+ " " + string(data.Key) + " " +  string(data.Value) + " " + data.Command
                f, _ := os.OpenFile("databaseLogs.txt", os.O_APPEND|os.O_WRONLY, 0644)
                fmt.Fprintln(f, backUp) //data entry
                id ++

            } else {
                logStandardOutput("Couldnt find the key to delete in main store: " + data.Key)
            }
        }
    }
}

//TCP
func serveTcp(portNumber string) {
    tcpListener, tcpErr := net.Listen("tcp4", portNumber)
    if tcpErr != nil {
        logError("Error:: ", tcpErr)
        //fmt.Println("Error : ", tcpErr) //error
        return
    }
    logStandardOutput("TCP Server is running on: " + tcpListener.Addr().String())
    //fmt.Println("TCP Server is running... ")
    for {
        LocalStore := make(map[string]string)
        for key, value := range Store {
            LocalStore[key] = value
        }
        tcpConnection, tcpConnectionErr := tcpListener.Accept()
        if tcpConnectionErr != nil {
            logError("Error:: ", tcpConnectionErr)
            //fmt.Println("Error : ", tcpConnectionErr) //error
            return
        }

        logStandardOutput("TCP connection made: "+ tcpConnection.RemoteAddr().String())
        //fmt.Println("TCP remote address: ")

        defer tcpListener.Close()
        go handleTcp(tcpConnection, LocalStore)
    }

}

func handleTcp(tcpConnection net.Conn, LocalStore map[string]string) {
    for {
        tcpNetData, err := bufio.NewReader(tcpConnection).ReadString('\n')
        if err != nil {
            logError("Error reading in from the connection " + tcpConnection.LocalAddr().String(), err)
            //fmt.Println("Error reading in from the connection: ", err, tcpConnection.LocalAddr()) //error
            return
        }
        storeKeyValue := storage.Message{}
        json.Unmarshal([]byte(tcpNetData), &storeKeyValue)
        logRequest(storeKeyValue, "TCP")

        manageRequest(storeKeyValue, tcpConnection, LocalStore)

        //fmt.Print("Local Store ", LocalStore, "\n") //needed?

    }
}
// UDP
func serveUDP(portNumber string) {
    udpAddr, udpErr := net.ResolveUDPAddr("udp4", portNumber)
    if udpErr != nil {
        logError("Error:: ", udpErr)
        //fmt.Println("Error : ", udpErr) //error
        return
    }
    //fmt.Println("UDP Server is running... ")
    udpConnection, udpConnectionErr := net.ListenUDP("udp4", udpAddr)
    if udpConnectionErr != nil {
        logError("Error:: ", udpConnectionErr)
        //fmt.Println("Error : ", udpConnectionErr) //error
        return
    }
    logStandardOutput("UDP Server is running on: " + udpConnection.LocalAddr().String())
    //logStandardOutput("UDP connection made to: " + udpConnection.LocalAddr().String())
    //fmt.Println("UDP connection made to: ", udpConnection.LocalAddr())
    defer udpConnection.Close()
    buffer := make([]byte, 1024)
    LocalStore := make(map[string]string)
    for key, value := range Store {
        LocalStore[key] = value
    }
    for {
        handleUDP(buffer, udpConnection, LocalStore)
    }


}

func handleUDP(buffer []byte, udpConnection *net.UDPConn, LocalStore map[string]string) {
    udpNetData, udpAddr, udpErr := udpConnection.ReadFromUDP(buffer)

    if udpErr != nil {
        logError("Error:: ", udpErr)
        //fmt.Println("Error : ", udpErr) //error
        return
    }
    logStandardOutput("UDP connection made to: " + udpConnection.LocalAddr().String())

    storeKeyValue := storage.Message{}
    err := json.Unmarshal([]byte(buffer[0:udpNetData]), &storeKeyValue) //error needs adding
    if err != nil {
        logError("Error:: ", err)
    }
    logRequest(storeKeyValue, "UDP")

    manageRequestUdp(storeKeyValue, udpConnection, udpAddr, LocalStore)

    //fmt.Print("Local Store ", LocalStore, "\n") // printing store needed?

}

func manageRequestUdp(request storage.Message, udpConnection *net.UDPConn, udpAddr *net.UDPAddr, LocalStore map[string]string) {
    ResponseChannel := make(chan string, 1)
    defer close(ResponseChannel)
    time.Sleep(1 * time.Second)
    switch request.Command {
    case "GET":
        GetUseCase(request.Key, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "UDP")
        udpConnection.WriteToUDP([]byte(output), udpAddr)
    case "PUT":
        PutUseCase(request, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "UDP")
        udpConnection.WriteToUDP([]byte(output), udpAddr)
    case "POST":
        PostUseCase(request, ResponseChannel, LocalStore)
        output := <-ResponseChannel
        logResponse(output, "UDP")
        udpConnection.WriteToUDP([]byte(output), udpAddr)
    case "DELETE":
        DeleteUseCase(request, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "UDP")
        udpConnection.WriteToUDP([]byte(output), udpAddr)
    case "GETALL":
        GetAllUseCase(ResponseChannel)
        output := <- ResponseChannel
        logResponse(output, "UDP")
        udpConnection.WriteToUDP([]byte(output), udpAddr)
    default:
        defaultMessage := fmt.Sprintf("Please send one of these commands: [GET, POST, PUT, DELETE]. This command doesnt exist: %v  ", request.Command)
        udpConnection.WriteToUDP([]byte(defaultMessage), udpAddr)
    }
}

func manageRequest(request storage.Message, connection net.Conn, LocalStore map[string]string) {
    ResponseChannel := make(chan string, 1)
    defer close(ResponseChannel)
    switch request.Command {
    case "GET":
        GetUseCase(request.Key, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "TCP")
        output = output + "\n"
        connection.Write([]byte(output))
    case "PUT":
        PutUseCase(request, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "TCP")
        output = output + "\n"
        connection.Write([]byte(output))
    case "POST":
        PostUseCase(request, ResponseChannel, LocalStore)
        output := <-ResponseChannel
        logResponse(output, "TCP")
        output = output + "\n"
        connection.Write([]byte(output))
    case "DELETE":
        DeleteUseCase(request, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        output = output + "\n"
        logResponse(output, "TCP")
        connection.Write([]byte(output))
    case "GETALL":
        GetAllUseCase(ResponseChannel)
        output := <- ResponseChannel
        logResponse(output, "TCP")
        output = output + "\n"
        connection.Write([]byte(output))
    default:
        defaultMessage := fmt.Sprintf("Please send one of these commands: [GET, POST, PUT, DELETE]. This command doesnt exist: %v  ", request.Command + "\n")
        logResponse(defaultMessage, "TCP")
        connection.Write([]byte(defaultMessage))
    }
}

func GetUseCase(key string, ResponseChannel chan string, LocalStore map[string]string, attempt string) {
    logStandardOutput("Searching for: " + key)
    //fmt.Println("Searching for: ", key)
    value, ok := LocalStore[key]
    if ok  {
        logStandardOutput("Found: " + key)
        //fmt.Println("Found: ", key)
        mapResult := map[string]string{
            key:value,
        }
        marshalKVStore, _ := json.Marshal(mapResult)
        ResponseChannel<-("Success: " + string(marshalKVStore))

    } else {
        logStandardOutput("on the rerun for key: " + key)
        //fmt.Println("on the rerun")
        if attempt == "rerun" {
            ResponseChannel <- ("Key not found: "+ key)
            return
        }
        updateLocalStoreFromMain(LocalStore)
        GetUseCase(key, ResponseChannel, LocalStore, "rerun")
    }
}

func PutUseCase(request storage.Message, ResponseChannel chan string, LocalStore map[string]string, attempt string) {
    key := request.Key
    logStandardOutput("Searching for: " + key)
    //fmt.Println("Searching for: ", key)
    value, ok := LocalStore[key]
    if ok {
        logStandardOutput("Updating KEY: " + key)
        //fmt.Println("Updating KEY: ", key)
        previousVersion := map[string]string{key:value,}
        LocalStore[key] = request.Value
        marshalKVStore, _ := json.Marshal(map[string]string{request.Key: request.Value})
        previousMarshalKVStore, _ := json.Marshal(previousVersion)
        logStandardOutput(string(marshalKVStore))
        //fmt.Println(marshalKVStore)

        var builderOutput strings.Builder
        builderOutput.WriteString("SUCCESS: updated: ")
        builderOutput.Write(previousMarshalKVStore)
        builderOutput.WriteString(" to: ")
        builderOutput.Write(marshalKVStore)
        logStandardOutput(builderOutput.String())
        //fmt.Println(builderOutput.String())
        // fmt.Println(message)
        StoreChannel <- request
        ResponseChannel <- builderOutput.String()
        return
    } else {
        logStandardOutput("on the rerun for key: " + key)
        //fmt.Println("on the rerun")
        if attempt == "rerun" {
            ResponseChannel <- ("Key not found: "+ key)
            return
        }
        updateLocalStoreFromMain(LocalStore)
        PutUseCase(request, ResponseChannel, LocalStore, "rerun")
    }
    ResponseChannel <- "Couldnt find the key: " + key
}

func PostUseCase(request storage.Message, ResponseChannel chan string, LocalStore map[string]string) {
    exist := doesKeyExist(request.Key)
    if exist {
        ResponseChannel <- ("That Key already exists: " + request.Key)
    }

    LocalStore[request.Key] = request.Value
    marshalKVStore, _ := json.Marshal(request)
    StoreChannel <- request
    ResponseChannel <- ("Success: " + string(marshalKVStore))
}

func DeleteUseCase(request storage.Message, ResponseChannel chan string, LocalStore map[string]string, attempt string) {
    key := request.Key
    logStandardOutput("Searching for: " + key)
    //fmt.Println("Searching for: ", key)
    _, ok := LocalStore[key]
    if ok {
        logStandardOutput("Found KEY: " + key)
        logStandardOutput("Deleting KEY: " + key)

        //fmt.Println("Found KEY: ", key)
        //fmt.Println("Deleting KEY: ", key)
        delete(LocalStore, key)
        output, _ := json.Marshal(LocalStore)
        StoreChannel <- request
        ResponseChannel <- ("Success: " + string(output))
    } else {
        logStandardOutput("on the rerun")

        //fmt.Println("on the rerun")
        if attempt == "rerun" {
            ResponseChannel <- ("Key not found: "+ key)
            return
        }
        updateLocalStoreFromMain(LocalStore)
        DeleteUseCase(request, ResponseChannel, LocalStore, "rerun")
    }
    ResponseChannel<- ("Couldnt find the key: " + key)

}

func GetAllUseCase(ResponseChannel chan string) {
    marshalKVStore, _ := json.Marshal(Store)
    logStandardOutput(string(marshalKVStore))
    //fmt.Println(marshalKVStore)
    message := []byte("The Store is: ")
    byteMarshalStore := []byte(marshalKVStore)
    output := append(message, byteMarshalStore...)
    ResponseChannel <- string(output)
}

func logRequest(request storage.Message, gateway string) {
    log.Printf("<<< %+v, | VIA: %v", request, gateway)
}
//
// func writeRequest(request storage.Message,gateway string, id int) {
//     f, _ := os.OpenFile("incomming_requests.txt", os.O_APPEND|os.O_WRONLY, 0644)
//     fmt.Fprintln(f, backUp)
// }
func logStandardOutput(output string) {
    log.Printf("--- %v", output)
}

func logResponse(response, gateway string) {
    log.Printf(">>> %s, | VIA: %s", response, gateway)
}
//
// func writeResponse(response ,gateway string, id int) {
//     f, _ := os.OpenFile("outgoing_response.txt", os.O_APPEND|os.O_WRONLY, 0644)
//     fmt.Fprintln(f, backUp)
// }

func logError(errorMessage string, error error) {
    log.Printf("*** %v :: %v", errorMessage, error)
}

func doesKeyExist(newKey string) bool {
    _, ok := Store[newKey]
    return ok
}

//http starts here:
func homePage(w http.ResponseWriter, r *http.Request){

    fmt.Fprintf(w, "Welcome to the HomePage!")
    logStandardOutput("Endpoint Hit: homePage")
    //fmt.Println("Endpoint Hit: homePage")
}

func returnAllStorage(w http.ResponseWriter, r *http.Request) {
    logStandardOutput("Endpoint Hit: stored")
    w.Header().Add("Content-Type", "application/json")

    //fmt.Println("Endpoint Hit: stored")
    json.NewEncoder(w).Encode(Store)

}

type httpStore struct {
    LocalStore map[string]string
}

func NewHttpStore() *httpStore {
    bufferStore := make(map[string]string)
    for key, value := range Store {
        bufferStore[key] = value
    }
    return &httpStore{LocalStore: bufferStore}
}
func (httpStore *httpStore )handleRequestsToStorage(w http.ResponseWriter, r *http.Request) {
    //fmt.Println("this is main store: ", Store)
    //fmt.Printf("This is the request :")
    //fmt.Println(r)
    //fmt.Printf("This is the method of the request : ")
    //fmt.Println(r.Method)
    LocalStore := httpStore.LocalStore
    reqBody, _ := ioutil.ReadAll(r.Body)
    var storeKeyValue storage.Message
    json.Unmarshal(reqBody, &storeKeyValue)
    //fmt.Printf("This is the decoded storage struct of the request : ")
    //timer.start
    logRequest(storeKeyValue, "HTTP")
    //fmt.Println(storeKeyValue)
    //fmt.Printf("This is the request body of the request : ")
    //fmt.Println(string(reqBody))
    //fmt.Println("this is http local store: ", LocalStore)

    ResponseChannel := make(chan string)
    switch r.Method {
    case "GET":
        go GetUseCase(storeKeyValue.Key, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "HTTP")
        // timer end add the request and time to
        output = output + "\n"
        json.NewEncoder(w).Encode(output) //handle error
    case "PUT":
        storeKeyValue.Command = "PUT"
        go PutUseCase(storeKeyValue, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        logResponse(output, "HTTP")
        output = output + "\n"
        json.NewEncoder(w).Encode(output) //handle error
    case "POST":
        storeKeyValue.Command = "POST"
        go PostUseCase(storeKeyValue, ResponseChannel, LocalStore)
        output := <-ResponseChannel
        logResponse(output, "HTTP")
        output = output + "\n"
        json.NewEncoder(w).Encode(output)//handle error
    case "DELETE":
        storeKeyValue.Command = "DELETE"
        go DeleteUseCase(storeKeyValue, ResponseChannel, LocalStore, "first")
        output := <-ResponseChannel
        output = output + "\n"
        logResponse(output, "HTTP")
        json.NewEncoder(w).Encode(output)//handle error
    default:
        defaultMessage := fmt.Sprintf("Please send one of these commands: [GET, POST, PUT, DELETE]. This command doesnt exist: %v  ", r.Method + "\n")
        logResponse(defaultMessage, "HTTP")
        json.NewEncoder(w).Encode(defaultMessage) //handle error
    }
}

func handleRequests(port string) {
    httpStore := NewHttpStore()
    http.HandleFunc("/", homePage)
    http.HandleFunc("/storages", returnAllStorage)
    http.HandleFunc("/storage", httpStore.handleRequestsToStorage)

    // http.HandleFunc("/storage/", returnOneKeyValueStore)

    log.Fatal(http.ListenAndServe(port, nil))

}

func updateLocalStoreFromMain(LocalStore map[string]string){
    for key, value := range Store {
        LocalStore[key] = value
    }
}
