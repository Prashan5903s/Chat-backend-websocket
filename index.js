require('dotenv').config()
const http = require('http')
const cors = require('cors')
const axios = require('axios')
const WebSocket = require('ws')
const mysql = require('mysql2')
const express = require('express')
const moment = require('moment-timezone')
const { format, utcToZonedTime } = require('date-fns-tz')

const app = express()
const PORT = 3001

// Connect to the database
const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 20
})

db.getConnection(err => {
  if (err) {
    console.error('Error connecting to the database:', err)
    process.exit(1) // Exit the application if the database connection fails
  } else {
    console.log('Connected to the database!')
  }
})

// Create a WebSocket server
const wss = new WebSocket.Server({ noServer: true })

wss.on('connection', (ws, req) => {
  console.log('New client connected')

  ws.on('message', message => {
    try {
      const data = JSON.parse(message)

      if (data.sendType === 'auth') {
        // Authenticate the user
        ws.userId = data.senderId

        var isGroup = data.isGroup ? 1 : 0

        // Fetch past messages
        const query = `
                    SELECT um.*, 
                           u1.first_name AS sender_first_name, u1.last_name AS sender_last_name, 
                           u2.first_name AS reciever_first_name, u2.last_name AS reciever_last_name
                    FROM user_message um
                    LEFT JOIN users u1 ON um.sender_id = u1.id
                    LEFT JOIN users u2 ON um.reciever_id = u2.id
                    WHERE ((um.reciever_id = ? AND um.sender_id = ?) 
                        OR (um.group_id = ?)
                        OR (um.reciever_id = ? AND um.sender_id = ?))
                    AND um.type = ? 
                    ORDER BY um.sent_time ASC
                `

        db.query(
          query,
          [
            data.recieverId,
            data.senderId,
            data.recieverId,
            data.senderId,
            data.recieverId,
            isGroup
          ],
          (err, results) => {
            if (err) {
              console.error('Database error fetching messages: ', err)
              return
            }

            results.forEach(msg => {
              // Add safety checks to ensure sender_id and reciever_id are valid
              const sender_id = msg.sender_id || null
              const reciever_id = msg.reciever_id || null
              const sender_name = `${msg.sender_first_name || 'Unknown'} ${
                msg.sender_last_name || ''
              }`.trim()
              const reciever_name = `${msg.reciever_first_name || 'Unknown'} ${
                msg.reciever_last_name || ''
              }`.trim()

              ws.send(
                JSON.stringify({
                  type: msg.type,
                  sendType: 'previous_message',
                  sender_id: sender_id,
                  sender_name: sender_name,
                  reciever_name: reciever_name,
                  image_url: msg.image_url,
                  receiver_id: reciever_id,
                  content: msg.message_text,
                  sent_time: msg.sent_time,
                  id: msg.id,
                  sender: data.senderId
                })
              )
            })
          }
        )
      } else if (data.sendType == 'userInfo') {
        ws.userId = data.senderId

        // Query for users
        const userQuery = `
                    SELECT * FROM users 
                    WHERE master_id = ? 
                    ORDER BY created_at ASC;
                `

        const driverQuery = `
                        SELECT * FROM users 
                        WHERE master_id = ? 
                        AND NOT id = ?
                        ORDER BY created_at ASC;
                    `

        const masterQuery = `
                    SELECT * FROM users
                    WHERE id = ?
                    AND user_type = 'TR'
                    ORDER BY created_at ASC;
                `

        // Query for groups created by the user
        const groupQuery = `
                    SELECT 
                        g.group_id, 
                        g.group_name, 
                        g.created_by, 
                        g.created_at,
                        ug.user_id AS user_group_user_id,   -- From user_group (may be NULL)
                        u.first_name AS user_first_name,    -- From users (may be NULL)
                        u.last_name AS user_last_name
                    FROM 
                        groups g
                    LEFT JOIN 
                        user_group ug ON g.group_id = ug.group_id  -- Include groups even if no user_group entry exists
                    LEFT JOIN 
                        users u ON ug.user_id = u.id              -- Include user details if available
                    WHERE 
                        g.created_by = ?
                    ORDER BY 
                        g.group_id DESC;
                `

        // Query for the user's groups (user_group relationship)
        const userGroupQuery = `
                    SELECT 
                        ug.*, 
                        g.*
                    FROM 
                        user_group ug
                    JOIN 
                        groups g ON ug.group_id = g.group_id
                    WHERE 
                        ug.user_id = ?
                    ORDER BY
                     ug.id ASC
                `

        // Fetch users
        db.query(userQuery, [data.senderId], (err, userResults) => {
          if (err) {
            console.error('Database error fetching users: ', err)
            return
          }

          // Send user listing results
          userResults.forEach(msg => {
            ws.send(
              JSON.stringify({
                id: msg.id,
                first_name: msg.first_name,
                last_name: msg.last_name,
                email: msg.email,
                type: 0,
                created_at: msg.created_at,
                sendType: 'user_list',
                image_url: msg.avatar_image
              })
            )
          })

          // Fetch groups created by the user
          db.query(groupQuery, [data.senderId], (err, groupResults) => {
            if (err) {
              console.error('Database error fetching groups: ', err)
              return
            }

            // Track sent group IDs to prevent duplicates
            const sentGroups = new Set()

            // Send group listing results, ensuring no duplicates
            groupResults.forEach(group => {
              if (!sentGroups.has(group.group_id)) {
                ws.send(
                  JSON.stringify({
                    type: 1,
                    id: group.group_id,
                    group_name: group.group_name,
                    created_by: group.created_by,
                    created_at: group.created_at,
                    user_id: group.user_group_user_id, // May be NULL
                    user_name:
                      group.user_first_name && group.user_last_name
                        ? `${group.user_first_name} ${group.user_last_name}` // Concatenate name if available
                        : null, // No associated user
                    sendType: 'group_list'
                  })
                )

                // Add the group_id to the sent set to avoid sending it again
                sentGroups.add(group.group_id)
              }
            })
          })

          db.query(
            driverQuery,
            [data.masterId, data.senderId],
            (err, driverData) => {
              if (err) {
                console.error('Database error fetching driver data: ', err)
                return
              }

              const sentDriverGroups = new Set()

              driverData.forEach(userGroup => {
                if (!sentDriverGroups.has(userGroup.id)) {
                  ws.send(
                    JSON.stringify({
                      id: userGroup.id,
                      first_name: userGroup.first_name,
                      last_name: userGroup.last_name,
                      email: userGroup.email,
                      type: 0, // Adjust as needed
                      created_at: userGroup.created_at,
                      sendType: 'driver_list',
                      image_url: userGroup.avatar_image // Ensure this field exists
                    })
                  )

                  // Track the sent user to avoid duplicates
                  sentDriverGroups.add(userGroup.id)
                }
              })
            }
          )

          db.query(masterQuery, [data.masterId], (err, driverData) => {
            if (err) {
              console.error('Database error fetching driver data: ', err)
              return
            }

            const sentDriverGroups = new Set()

            driverData.forEach(userGroup => {
              if (!sentDriverGroups.has(userGroup.id)) {
                ws.send(
                  JSON.stringify({
                    id: userGroup.id,
                    first_name: userGroup.first_name,
                    last_name: userGroup.last_name,
                    email: userGroup.email,
                    type: 0, // Adjust as needed
                    created_at: userGroup.created_at,
                    sendType: 'master_list',
                    image_url: userGroup.avatar_image // Ensure this field exists
                  })
                )

                // Track the sent user to avoid duplicates
                sentDriverGroups.add(userGroup.id)
              }
            })
          })

          // Fetch user's groups (user_group relationship)
          db.query(userGroupQuery, [data.senderId], (err, userGroupResults) => {
            if (err) {
              console.error('Database error fetching user groups: ', err)
              return
            }

            // Track sent group IDs to prevent duplicates
            const sentUserGroups = new Set()

            // Send user-group listing results, ensuring no duplicates
            userGroupResults.forEach(userGroup => {
              if (!sentUserGroups.has(userGroup.group_id)) {
                ws.send(
                  JSON.stringify({
                    type: 1, // Assuming 2 represents user-specific group data
                    id: userGroup.group_id,
                    group_name: userGroup.group_name,
                    created_by: userGroup.created_by,
                    created_at: userGroup.created_at,
                    user_id: userGroup.user_id, // Specific user-group entry
                    sendType: 'user_group_list'
                  })
                )

                // Add the group_id to the sent set to avoid sending it again
                sentUserGroups.add(userGroup.group_id)
              }
            })
          })
        })
      } else if (data.sendType == 'group_create') {
        ws.userId = data.senderId

        // Insert the new group into the 'groups' table
        const query = `
                    INSERT INTO groups (group_name, master_id, master_company_id, created_by, is_active)
                    VALUES (?, ?, ?, ?, ?)
                `

        // Save the group into the 'groups' table
        db.query(
          query,
          [
            data.groupName,
            data.masterId,
            data.masterCompanyId, // Assuming master_company_id is the same as master_id
            data.ids,
            1 // Assuming the group is active by default
          ],
          (err, result) => {
            if (err) {
              console.error('Database error saving group: ', err)
              return
            }

            const groupId = result.insertId // Get the ID of the newly created group

            // Insert the association between users and the newly created group into the 'user_group' table
            const groupCreateQuery = `
                        INSERT INTO user_group (group_id, user_id, is_active)
                        VALUES (?, ?, ?)
                    `

            // Loop through the user_id array and insert each user into the 'user_group' table
            data.userSelected.forEach(userId => {
              db.query(
                groupCreateQuery,
                [
                  groupId,
                  userId,
                  1 // Assuming the user is active by default
                ],
                (err, result) => {
                  if (err) {
                    console.error(
                      'Database error saving user-group association: ',
                      err
                    )
                    return
                  }
                }
              )
            })

            wss.clients.forEach(client => {
              client.send(
                JSON.stringify({
                  type: 1,
                  group_id: groupId,
                  sendType: 'group_list',
                  group_name: data.groupName,
                  user_id: data.userSelected,
                  created_by: data.ids,
                  created_at: ''
                })
              )
            })
          }
        )
      } else if (data.sendType === 'message') {
        if (data.type) {
          // Save the message
          const query = `
                        INSERT INTO user_message (type, sender_id, group_id, image_url, message_text, master_id, master_company_id, created_by, sent_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `

          db.query(
            query,
            [
              data.type,
              data.sender_id,
              data.reciever_id,
              data.image_url,
              data.content,
              data.master_id,
              data.master_company_id,
              data.sender_id,
              data.sent_time
            ],
            (err, result) => {
              if (err) {
                console.error('Database error saving message: ', err)
                return
              }

              // Query to get the sender's name
              const senderQuery = `
                                SELECT first_name, last_name
                                FROM users
                                WHERE id = ?
                            `

              db.query(senderQuery, [data.sender_id], (err, senderResult) => {
                if (err) {
                  console.error('Error fetching sender details: ', err)
                  return
                }

                const sender_name =
                  senderResult.length > 0
                    ? `${senderResult[0].first_name || 'Unknown'} ${
                        senderResult[0].last_name || ''
                      }`.trim()
                    : 'Unknown'

                // Broadcast the message to the recipient with sender_name
                wss.clients.forEach(client => {
                  client.send(
                    JSON.stringify({
                      type: data.type,
                      sendType: 'new_message',
                      reciever_id: data.reciever_id,
                      sender_id: data.sender_id,
                      sender_name: sender_name,
                      reciever_name: null,
                      image_url: data.image_url,
                      content: data.content,
                      sent_time: data.sent_time
                    })
                  )
                })
              })
            }
          )
        } else {
          const query = `
                        INSERT INTO user_message (type, sender_id, reciever_id, image_url, message_text, master_id, master_company_id, created_by, sent_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `

          db.query(
            query,
            [
              data.type,
              data.sender_id,
              data.reciever_id,
              data.image_url,
              data.content,
              data.master_id,
              data.master_company_id,
              data.sender_id,
              data.sent_time
            ],
            (err, result) => {
              if (err) {
                console.error('Database error saving message: ', err)
                return
              }

              // Query to get sender and receiver names
              const senderReceiverQuery = `
                            SELECT 
                                u1.first_name AS sender_first_name, u1.last_name AS sender_last_name, 
                                u2.first_name AS reciever_first_name, u2.last_name AS reciever_last_name
                            FROM users u1
                            LEFT JOIN users u2 ON u2.id = ? 
                            WHERE u1.id = ?
                        `

              db.query(
                senderReceiverQuery,
                [data.reciever_id, data.sender_id],
                (err, namesResult) => {
                  if (err) {
                    console.error('Error fetching sender/receiver names: ', err)
                    return
                  }

                  const sender_name =
                    namesResult.length > 0
                      ? `${namesResult[0].sender_first_name || 'Unknown'} ${
                          namesResult[0].sender_last_name || ''
                        }`.trim()
                      : 'Unknown'
                  const reciever_name =
                    namesResult.length > 0
                      ? `${namesResult[0].reciever_first_name || 'Unknown'} ${
                          namesResult[0].reciever_last_name || ''
                        }`.trim()
                      : 'Unknown'

                  // Broadcast the message to all clients
                  wss.clients.forEach(client => {
                    client.send(
                      JSON.stringify({
                        type: data.type,
                        sendType: 'new_message',
                        reciever_id: data.reciever_id,
                        sender_id: data.sender_id,
                        sender_name: sender_name,
                        reciever_name: reciever_name,
                        image_url: data.image_url,
                        content: data.content,
                        sent_time: data.sent_time
                      })
                    )
                  })
                }
              )
            }
          )
        }
      } else if (data.sendType === 'totalMsg') {
        ws.userId = data.senderId

        // Fetch past messages with is_read = 0
        const query = `
                    SELECT * FROM user_message 
                    WHERE (
                        (reciever_id = ? AND reciever_id != 0 AND is_read = 0) 
                        OR 
                        (reciever_id = 0)
                    )   
                    ORDER BY sent_time DESC
                `

        db.query(query, [data.receiverId], (err, results) => {
          if (err) {
            console.error('Database error fetching messages: ', err)
            return
          }

          // Query to get sender and receiver names
          const senderReceiverQuery = `
                            SELECT 
                                u1.first_name AS sender_first_name, u1.last_name AS sender_last_name, 
                                u2.first_name AS reciever_first_name, u2.last_name AS reciever_last_name
                            FROM users u1
                            LEFT JOIN users u2 ON u1.id = ? AND u2.id = ?
                        `

          results.forEach(msg => {
            db.query(
              senderReceiverQuery,
              [msg.sender_id, msg.reciever_id],
              (err, namesResult) => {
                if (err) {
                  console.error('Error fetching sender/receiver names: ', err)
                  return
                }

                const saltLakeCityTime = moment
                  .tz(msg.sent_time, 'Asia/Kolkata')
                  .format('YYYY-MM-DD HH:mm:ss')

                const sender_name =
                  namesResult.length > 0
                    ? `${namesResult[0].sender_first_name || 'Unknown'} ${
                        namesResult[0].sender_last_name || ''
                      }`.trim()
                    : 'Unknown'
                const reciever_name =
                  namesResult.length > 0
                    ? `${namesResult[0].reciever_first_name || 'Unknown'} ${
                        namesResult[0].reciever_last_name || ''
                      }`.trim()
                    : 'Unknown'

                // Send the message with sender and receiver names
                ws.send(
                  JSON.stringify({
                    id: msg.id,
                    type: msg.type,
                    sendType: 'totalMsg',
                    is_read: msg.is_read,
                    sender: data.senderId,
                    group_id: msg.group_id,
                    sent_time: saltLakeCityTime,
                    sender_id: msg.sender_id,
                    image_url: msg.image_url,
                    sender_name: sender_name,
                    content: msg.message_text,
                    reciever_name: reciever_name,
                    receiver_id: msg.reciever_id
                      ? msg.reciever_id
                      : msg.group_id
                  })
                )
              }
            )
          })
        })
      } else if (data.sendType === 'update_read_status') {
        // Query to update `is_read` status
        const updateQuery = `
                    UPDATE user_message
                    SET is_read = 1
                    WHERE (
                        (group_id = 0 AND reciever_id = ? AND sender_id = ?) 
                        OR 
                        (reciever_id = 0 AND sender_id = ? AND group_id = ?)
                    )
                `

        if (data.isGroup) {
          if (data.useType) {
            // [data.recieverId, data.recieverId, data.recieverId, data.recieverId, data.senderId],

            const groupUpdateQuery = `
                        UPDATE user_message
                        SET is_read = CASE
                                        WHEN is_read = '0' THEN ?
                                        WHEN FIND_IN_SET(?, is_read) = 0 THEN CONCAT(is_read, ',', ?)
                                        ELSE is_read
                                      END
                        WHERE  
                            reciever_id = 0 
                            AND sender_id != ? 
                            AND group_id = ?;
                    `

            db.query(
              groupUpdateQuery,
              [
                data.recieverId,
                data.recieverId,
                data.recieverId,
                data.recieverId,
                data.senderId
              ],
              (err, updateResults) => {
                if (err) {
                  console.error('Database error updating messages: ', err)
                  ws.send(
                    JSON.stringify({
                      type: 'error',
                      sendType: 'update_read_status',
                      message: 'Failed to update read status'
                    })
                  )
                  return
                }

                // Notify all WebSocket clients about the update
                wss.clients.forEach(client => {
                  client.send(
                    JSON.stringify({
                      type: data.isGroup ? 1 : 0,
                      sendType: 'message_read_status',
                      reciever_id: data.recieverId,
                      sender_id: data.senderId,
                      id: client.userId,
                      sent_time: data.sent_time
                    })
                  )
                })
              }
            )
          } else {
            const groupUpdateQuery = `
                        UPDATE user_message
                        SET is_read = CASE
                                        WHEN is_read = '0' THEN ?
                                        WHEN FIND_IN_SET(?, is_read) = 0 THEN CONCAT(is_read, ',', ?)
                                        ELSE is_read
                                      END
                        WHERE  
                            reciever_id = 0 
                            AND sender_id != ? 
                            AND group_id = ?;
                    `

            db.query(
              groupUpdateQuery,
              [data.id, data.id, data.id, data.id, data.senderId],
              (err, updateResults) => {
                if (err) {
                  console.error('Database error updating messages: ', err)
                  ws.send(
                    JSON.stringify({
                      type: 'error',
                      sendType: 'update_read_status',
                      message: 'Failed to update read status'
                    })
                  )
                  return
                }

                // Notify all WebSocket clients about the update
                wss.clients.forEach(client => {
                  client.send(
                    JSON.stringify({
                      type: data.isGroup ? 1 : 0,
                      sendType: 'message_read_status',
                      reciever_id: data.recieverId,
                      sender_id: data.senderId,
                      id: client.userId,
                      sent_time: data.sent_time
                    })
                  )
                })
              }
            )
          }
        } else {
          // Step 1: Update the `is_read` status
          db.query(
            updateQuery,
            [data.recieverId, data.senderId, data.senderId, data.recieverId],
            (err, updateResults) => {
              if (err) {
                console.error('Database error updating messages: ', err)

                ws.send(
                  JSON.stringify({
                    type: 'error',
                    sendType: 'update_read_status',
                    message: 'Failed to update read status'
                  })
                )

                return
              }

              // Step 2: Fetch unread messages after the update
              wss.clients.forEach(client => {
                client.send(
                  JSON.stringify({
                    type: data.isGroup ? 1 : 0,
                    sendType: 'message_read_status',
                    reciever_id: data.recieverId,
                    sender_id: data.senderId,
                    sent_time: data.sent_time
                  })
                )
              })
            }
          )
        }
      }
    } catch (err) {
      console.error('Error parsing message: ', err)
    }
  })

  ws.on('close', () => {
    console.log('Client disconnected')
  })
})

// ==========================================
// Express Middleware
// ==========================================

app.use(express.json())

app.post('/broadcast-duty-status', (req, res) => {
  try {

    const data = req.body

    

    const driverId = Number(data.driverId)

    let sent = false

    // Send only to the WebSocket client
    // whose userId matches driverId
    wss.clients.forEach(client => {
      if (
        client.readyState === WebSocket.OPEN &&
        Number(client.userId) === driverId
      ) {
        client.send(
          JSON.stringify({
            sendType: 'change-duty-status',
            driverId: data.driverId,
            vehicleId: data.vehicleId,
            shiftStatus: data.shiftStatus,
            startLogTime: data.startLogTime,
            endLogTime: data.endLogTime,
            locationName: data.locationName,
            odometer: data.odometer,
            engineHours: data.engineHours
          })
        )

        sent = true

        console.log(`Duty status sent to user ID: ${client.userId}`)
      }
    })

    return res.status(200).json({
      status: 'success',
      message: sent
        ? 'Duty status sent successfully'
        : 'Driver is not connected',
      driverId: driverId
    })
  } catch (error) {
    console.error('Error broadcasting duty status:', error)

    return res.status(500).json({
      status: 'failure',
      message: 'Failed to broadcast duty status'
    })
  }
})

// Integrate WebSocket with the Express server
const server = app.listen(PORT, () =>
  console.log(`Server running on port ${PORT}`)
)

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, ws => {
    wss.emit('connection', ws, request)
  })
})
