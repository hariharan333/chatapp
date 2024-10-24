package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

type Client struct {
	ID      string
	MsgChan chan string
}

type ChatRoom struct {
	mu        sync.Mutex
	clients   map[string]*Client
	broadcast chan string
}

var chatRoom = &ChatRoom{
	clients:   make(map[string]*Client),
	broadcast: make(chan string),
}

var db *sql.DB


// Start the chat room for broadcasting messages
func (c *ChatRoom) start() {
	for {
		msg := <-c.broadcast
		c.mu.Lock()
		for _, client := range c.clients {
			select {
			case client.MsgChan <- msg:
			default:
				// Avoid blocking, skip if the client's channel is full
			}
		}
		c.mu.Unlock()
	}
}

// Join the chat room after checking if the user exists in the database
func joinChatHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		http.Error(w, "Client ID required", http.StatusBadRequest)
		return
	}

	// Insert the new user into the database
	insertUserQuery := `INSERT INTO users (id) VALUES (?)`

	_, err := db.Exec(insertUserQuery, clientID)
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.Code == sqlite3.ErrConstraint {
			// This error occurs if the ID already exists (due to the PRIMARY KEY constraint)
			http.Error(w, "Client ID already exists", http.StatusConflict)
			return
		}
		http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}


	client := &Client{
		ID:      clientID,
		MsgChan: make(chan string, 10), // Buffered channel to avoid blocking
	}

	chatRoom.mu.Lock()
	chatRoom.clients[clientID] = client
	chatRoom.mu.Unlock()

	fmt.Fprintf(w, "Client %s joined the chat", clientID)
}

// Send a message to the chat room and store it in the DB
func sendMessageHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")
	message := r.URL.Query().Get("message")
	if clientID == "" || message == "" {
		http.Error(w, "Client ID and message required", http.StatusBadRequest)
		return
	}

	// Check if the user exists in the database
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)", clientID).Scan(&exists)
	if err != nil || !exists {
		http.Error(w, "Invalid client ID: Access Denied", http.StatusUnauthorized)
		return
	}

	// Store the message in the database
	_, err = db.Exec("INSERT INTO messages (client_id, message) VALUES (?, ?)", clientID, message)
	if err != nil {
		http.Error(w, "Failed to store message", http.StatusInternalServerError)
		return
	}

	// Broadcast the message to all connected clients
	chatRoom.broadcast <- fmt.Sprintf("%s: %s", clientID, message)
	fmt.Fprintf(w, "Message sent and stored")
}

// Leave the chat room
func leaveChatHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		http.Error(w, "Client ID required", http.StatusBadRequest)
		return
	}

	// Use Exec for DELETE queries
	result, err := db.Exec("DELETE FROM users WHERE id = ?", clientID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Database user deletion error: %v", err), http.StatusInternalServerError)
		return
	}

	// Check how many rows were affected by the delete operation
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		http.Error(w, "Error retrieving rows affected", http.StatusInternalServerError)
		return
	}

	if rowsAffected == 0 {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	fmt.Fprintf(w, "User %s successfully deleted", clientID)

	chatRoom.mu.Lock()
	if _, exists := chatRoom.clients[clientID]; exists {
		delete(chatRoom.clients, clientID)
	}
	chatRoom.mu.Unlock()

	fmt.Fprintf(w, "Client %s left the chat", clientID)
}

// Get all messages from the database based on the insertion time
func getMessagesHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		http.Error(w, "Client ID required", http.StatusBadRequest)
		return
	}

	// Check if the user exists in the database
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)", clientID).Scan(&exists)
	if err != nil || !exists {
		http.Error(w, "Invalid client ID: Access Denied", http.StatusUnauthorized)
		return
	}

	// Fetch all messages from the database sorted by timestamp
	rows, err := db.Query("SELECT client_id, message, timestamp FROM messages ORDER BY timestamp")
	if err != nil {
		http.Error(w, "Failed to retrieve messages", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var messages []map[string]string
	for rows.Next() {
		var clientID, message, timestamp string
		if err := rows.Scan(&clientID, &message, &timestamp); err != nil {
			http.Error(w, "Failed to scan messages", http.StatusInternalServerError)
			return
		}
		msg := map[string]string{
			"client_id": clientID,
			"message":   message,
			"timestamp": timestamp,
		}
		messages = append(messages, msg)
	}

	// Return messages as JSON response
	if err := json.NewEncoder(w).Encode(messages); err != nil {
		http.Error(w, "Failed to encode messages", http.StatusInternalServerError)
	}
}


func main() {
	var err error
	// Initialize SQLite database
	db, err = sql.Open("sqlite3", "./chat.db")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create the users and messages table if they don't exist
	createUsersTable := `CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY
	);`
	_, err = db.Exec(createUsersTable)
	if err != nil {
		log.Fatal("Failed to create users table:", err)
	}

	createMessagesTable := `CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		client_id TEXT,
		message TEXT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(createMessagesTable)
	if err != nil {
		log.Fatal("Failed to create messages table:", err)
	}

	http.HandleFunc("/join", joinChatHandler)
	http.HandleFunc("/send", sendMessageHandler)
	http.HandleFunc("/leave", leaveChatHandler)
	http.HandleFunc("/messages", getMessagesHandler)

	go chatRoom.start()

	log.Println("Chat server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}