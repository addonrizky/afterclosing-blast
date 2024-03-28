package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"net/http"
	"net/url"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

var apiHost string
var layananKonsumenHost string
var db *sql.DB

type ViaSSHDialer struct {
	client *ssh.Client
}

func (self *ViaSSHDialer) Dial(addr string) (net.Conn, error) {
	return self.client.Dial("tcp", addr)
}

type RespLanggengGetCmpgnByCode struct {
	Campaigns []struct {
		Campaign
	} `json:"campaigns"`
}

type Campaign struct {
	ID                     int       `json:"id"`
	UserID                 int       `json:"user_id"`
	Title                  string    `json:"title"`
	Description            string    `json:"description"`
	Code                   string    `json:"code"`
	CustomFields           string    `json:"custom_fields"`
	StartTime              string    `json:"start_time"`
	EndTime                string    `json:"end_time"`
	Status                 string    `json:"status"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
	CampaignUnsubscribeIds any       `json:"campaign_unsubscribe_ids"`
}

func main() {
	limitProcessPerExecutionRaw := os.Getenv("LIMIT_PROCESS_PER_EXECUTION")
	limitProcessPerExecution, err := strconv.Atoi(limitProcessPerExecutionRaw)
	if err != nil {
		fmt.Println("fail pars limit process per execution")
		return
	}

	if os.Getenv("CONNECTION_TYPE") == "TUNNEL" {
		conn, sshconn := connectDBwithTunnel()
		defer conn.Close()
		defer sshconn.Close()
	} else {
		connectDBBasic()
		db.Close()
	}

	yesterday := time.Now().AddDate(0, 0, -2)
	yesterdayDate := yesterday.Format("2006-01-02")

	nextMonth := time.Now().AddDate(0, 0, 30)
	nextMonthDate := nextMonth.Format("2006-01-02")

	cmpgnCode := os.Args[1]
	listAwarderRaw := os.Args[2]
	listAwarder := strings.Split(listAwarderRaw, ";")

	userIdRaw := listAwarder[0]
	userId, err := strconv.Atoi(userIdRaw)
	if err != nil {
		fmt.Println("eror on parsing userid : ", err)
		return
	}

	csAkuisisiRaw := listAwarder[1]
	csAkuisisi := strings.Split(csAkuisisiRaw, ",")

	campaigns, err := getCampaignsByCode(cmpgnCode)
	if err != nil {
		fmt.Println("error ah: ", err)
		return
	}

	for _, s := range campaigns.Campaigns {
		if s.UserID == userId {
			for _, firstName := range csAkuisisi {
				sendVoucherToAwardee(db, yesterdayDate, nextMonthDate, strconv.Itoa(0*limitProcessPerExecution), limitProcessPerExecutionRaw, s.Campaign, firstName)
			}
		}
	}

	db.Close()

	return
}

func callLanggeng(voucherCode, voucherKey, userId, name, phone, customerId, expiredAt, productName, campaignId, resi string) {
	params := url.Values{}
	params.Add("voucher_code", voucherCode)
	params.Add("voucher_key", voucherKey)
	params.Add("user_id", userId)
	params.Add("name", name)
	params.Add("phone", phone)
	params.Add("customer_id", customerId)
	params.Add("expired_at", expiredAt)
	params.Add("product_name", productName)
	params.Add("voucher_link", layananKonsumenHost+"voucher/"+voucherKey)
	params.Add("campaign_id", campaignId)
	params.Add("resi", resi)

	resp, err := http.PostForm(apiHost+"api/voucher/create", params)
	if err != nil {
		log.Printf("Request Failed: %s", err)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	// Log the request body
	bodyString := string(body)
	log.Print(bodyString)

	// Unmarshal result
	var best map[string]interface{}
	err = json.Unmarshal(body, &best)
	if err != nil {
		log.Printf("Reading body failed: %s", err)
		return
	}

	fmt.Println(best)
}

func getCampaignsByCode(code string) (RespLanggengGetCmpgnByCode, error) {
	resp, err := http.Get(apiHost + "api/campaigns/code/" + code)
	if err != nil {
		log.Printf("Request Failed: %s", err)
		return RespLanggengGetCmpgnByCode{}, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	// Log the request body
	bodyString := string(body)
	log.Print(bodyString)

	// Unmarshal result
	var respCmpgns RespLanggengGetCmpgnByCode
	err = json.Unmarshal(body, &respCmpgns)
	if err != nil {
		log.Printf("Reading body failed: %s", err)
		return RespLanggengGetCmpgnByCode{}, err
	}

	return respCmpgns, nil
}

func sendVoucherToAwardee(db *sql.DB, yesterdayDate string, nextMonthDate string, offset string, limitProcessPerExecution string, cmpgn Campaign, firstName string) error {
	rows, err := db.Query("select " +
		"a.order_id, " +
		"a.customer_id, " +
		"b.customer_name, " +
		"b.phone_1, " +
		"c.product_id, " +
		"d.product_name, " +
		"a.package_resi " +
		"from orders a " +
		"left join customers b ON a.customer_id = b.customer_id " +
		"left join order_detail c ON a.order_id = c.order_id " +
		"left join products d ON d.product_id = c.product_id " +
		"WHERE d.product_id = 'NBP008' AND date(a.created_at) = '" + yesterdayDate + "' " +
		"AND a.package_resi != '' " +
		"AND cs_first_name = '" + firstName + "' " +
		"group by b.phone_1 " +
		"limit " + limitProcessPerExecution + " offset " + offset,
	)

	if err != nil {
		fmt.Println("fail on query : ", err)
		return err
	}

	for rows.Next() {
		var orderID int64
		var customerID string
		var customerName string
		var phone string
		var productID string
		var productName string
		var resi string
		rows.Scan(&orderID, &customerID, &customerName, &phone, &productID, &productName, &resi)
		fmt.Printf("ID: %d  Name: %s\n", orderID, customerName)
		fmt.Println("customer phone :", phone)
		voucherKey := uuid.New()
		callLanggeng(strconv.Itoa(int(orderID)), voucherKey.String()[0:8], strconv.Itoa(cmpgn.UserID), customerName, cleanPhone(phone), customerID, nextMonthDate, productName, strconv.Itoa(cmpgn.ID), resi)
		randDelay := random(30, 60)
		time.Sleep(time.Duration(randDelay) * time.Second)
	}

	rows.Close()

	return nil
}

func connectDBwithTunnel() (conn net.Conn, sshcon *ssh.Client) {
	fmt.Println(os.Getenv("API_LANGGENG_URL"))
	fmt.Println(os.Getenv("LAYANAN_KONSUMEN_HBP_URL"))
	fmt.Println(os.Getenv("SSH_JUPYTER_HOST"))
	fmt.Println(os.Getenv("SSH_JUPYTER_PORT"))
	fmt.Println(os.Getenv("SSH_JUPYTER_USER"))
	fmt.Println(os.Getenv("SSH_JUPYTER_PASS"))
	fmt.Println(os.Getenv("DMS_DB_USER"))
	fmt.Println(os.Getenv("DMS_DB_PASS"))
	fmt.Println(os.Getenv("DMS_DB_HOST"))
	fmt.Println(os.Getenv("DMS_DB_NAME"))

	apiHost = os.Getenv("API_LANGGENG_URL")
	layananKonsumenHost = os.Getenv("LAYANAN_KONSUMEN_HBP_URL")
	sshHost := os.Getenv("SSH_JUPYTER_HOST") // SSH Server Hostname/IP
	sshPort := os.Getenv("SSH_JUPYTER_PORT") // SSH Port
	sshUser := os.Getenv("SSH_JUPYTER_USER") // SSH Username
	sshPass := os.Getenv("SSH_JUPYTER_PASS") // Empty string for no password
	dbUser := os.Getenv("DMS_DB_USER")       // DB username
	dbPass := os.Getenv("DMS_DB_PASS")       // DB Password
	dbHost := os.Getenv("DMS_DB_HOST")       // DB Hostname/IP
	dbName := os.Getenv("DMS_DB_NAME")       // Database name

	var agentClient agent.Agent
	// Establish a connection to the local ssh-agent
	conn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		fmt.Println("error on dialing ssh unix : ", err)
		return
	}
	// defer conn.Close()

	// Create a new instance of the ssh agent
	agentClient = agent.NewClient(conn)

	// The client configuration with configuration option to use the ssh-agent
	sshConfig := &ssh.ClientConfig{
		User:            sshUser,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// When the agentClient connection succeeded, add them as AuthMethod
	if agentClient == nil {
		fmt.Println("agent client connecton not succes")
		return
	}

	sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeysCallback(agentClient.Signers))
	// When there's a non empty password add the password AuthMethod
	if sshPass != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.PasswordCallback(func() (string, error) {
			return sshPass, nil
		}))
	}

	c := fmt.Sprintf("%s:%s", sshHost, sshPort)

	fmt.Println(c)

	// Connect to the SSH Server
	sshcon, err = ssh.Dial("tcp", fmt.Sprintf("%s:%s", sshHost, sshPort), sshConfig)
	if err != nil {
		fmt.Println("fail on connect to SSH Server : ", err)
		return
	}

	//defer sshcon.Close()

	// Now we register the ViaSSHDialer with the ssh connection as a parameter
	mysql.RegisterDial("mysql+tcp", (&ViaSSHDialer{sshcon}).Dial)

	// And now we can use our new driver with the regular mysql connection string tunneled through the SSH connection
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@mysql+tcp(%s)/%s", dbUser, dbPass, dbHost, dbName))
	if err != nil {
		fmt.Println("fail on connect regular mysql connection tunneled through SSH connection : ", err)
		return
	}

	fmt.Printf("Successfully connected to the db\n")

	return
}

func connectDBBasic() {
	fmt.Println(os.Getenv("API_LANGGENG_URL"))
	fmt.Println(os.Getenv("LAYANAN_KONSUMEN_HBP_URL"))
	fmt.Println(os.Getenv("DMS_DB_USER"))
	fmt.Println(os.Getenv("DMS_DB_PASS"))
	fmt.Println(os.Getenv("DMS_DB_HOST"))
	fmt.Println(os.Getenv("DMS_DB_NAME"))

	apiHost = os.Getenv("API_LANGGENG_URL")
	layananKonsumenHost = os.Getenv("LAYANAN_KONSUMEN_HBP_URL")
	dbUser := os.Getenv("DMS_DB_USER") // DB username
	dbPass := os.Getenv("DMS_DB_PASS") // DB Password
	dbHost := os.Getenv("DMS_DB_HOST") // DB Hostname/IP
	dbName := os.Getenv("DMS_DB_NAME") // Database name

	var err error
	// And now we can use our new driver with the regular mysql connection string tunneled through the SSH connection
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPass, dbHost, dbName))
	if err != nil {
		fmt.Println("fail on connect regular mysql connection tunneled through SSH connection : ", err)
		return
	}

	fmt.Printf("Successfully connected to the db\n")
}

func cleanPhone(phone string) string {
	return strings.Replace(phone, "+", "", -1)
}

func random(min int, max int) int {
	return rand.Intn(max-min) + min
}
