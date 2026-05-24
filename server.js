require("dotenv").config();
const express = require("express");
const { Server } = require("socket.io");
const http = require("http");
const cors = require("cors");
const cookieParser = require("cookie-parser");
// const User = require("./models/userModel");
const connectDB = require("./lib/db");
const userRouter = require("./routers/userRouter");
const messageRouter = require("./routers/messageRouter");
const connectionRouter = require("./routers/connectionRouter");
const globalErrorHandler = require("./utils/globalErrorHandler");
const User = require("./models/userModel");
const getNewChallenge = require("./utils/wordEngine");
const startNeonTimer = require("./utils/startWordsTimer");
const Message = require("./models/messageModel");
const { default: mongoose } = require("mongoose");

//REQUIRE THE SECURITY PACKAGES YOU JUST INSTALLED
const helmet = require("helmet");
const mongoSanitize = require("express-mongo-sanitize");
const hpp = require("hpp");

const app = express();

const allowedOrigins = [
  "http://localhost:3000",
  process.env.FRONTEND_URL, // Render will inject this automatically
];

//Wrapping express in http server
const server = http.createServer(app);

// helmet's cross-origin policies for your local development setup
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: false,
  }),
);

// This allows  origins from my FRONTEND APP
app.use(
  cors({
    origin: allowedOrigins,
    credentials: true,
  }),
);

// Image Limit
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

// app.use(
//   mongoSanitize({
//     allowDots: true,
//     excludedKeys: ["image", "avatar"], // ← skip sanitizing image fields
//   }),
// );

app.use(hpp());
app.use(cookieParser());

const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true,
  },
});

const userSocketMap = {};
const gameSessions = {};
const activeChallenge = {};
app.set("socketio", io);
app.set("userSocketMap", userSocketMap);

io.on("connection", (socket) => {
  // console.log("⚡ New connection established:", socket.id);

  socket.on("identify-user", async (userId) => {
    if (userId) {
      socket.userId = userId;
      userSocketMap[userId] = socket.id;
      // Join a room named after the MongoDB User ID
      socket.join(userId);
      // console.log(`User ${userId} joined their personal room.`);

      io.emit("update-online-users", Object.keys(userSocketMap));

      try {
        // 1. Find messages sent to THIS user that are still 'sent'
        const undeliveredMessages = await Message.find({
          receiverId: userId,
          status: "sent",
        });

        if (undeliveredMessages.length > 0) {
          // 2. Update to 'delivered' in MongoDB
          await Message.updateMany(
            { _id: { $in: undeliveredMessages.map((m) => m._id) } },
            { $set: { status: "delivered" } },
          );

          // 3. Notify the Senders
          undeliveredMessages.forEach((msg) => {
            const senderSocketId = userSocketMap[String(msg.senderId)];

            if (senderSocketId) {
              io.to(senderSocketId).emit("message-delivered-update", {
                messageId: msg._id,
                status: "delivered",
              });
            }
          });
        }
      } catch (err) {
        // console.error("Error updating delivery status on login:", err);
      }
    }

    socket.on("rejoin-game", ({ roomId, userId }) => {
      const session = gameSessions[roomId];

      if (!session) {
        socket.emit("opponent-left", {
          message: "This game session has expired or ended.",
        });
        return;
      }
      if (session) {
        socket.join(roomId);

        // 1. Update the Socket ID using String comparison
        if (String(session.player1.id) === String(userId)) {
          session.player1.socketId = socket.id;
        } else if (String(session.player2.id) === String(userId)) {
          session.player2.socketId = socket.id;
        }

        // 2. THE ULTIMATE FIX: Create a "POJO" (Plain Old JavaScript Object)
        // We manually pick only the data the frontend needs.
        const cleanSession = {
          player1: {
            id: String(session.player1.id),
            username: session.player1.username,
            avatar: session.player1.avatar,
            score: session.player1.score,
            active: session.player1.active,
          },
          player2: {
            id: String(session.player2.id),
            username: session.player2.username,
            avatar: session.player2.avatar,
            score: session.player2.score,
            active: session.player2.active,
          },
          scrambledWord: session.scrambledWord,
          timeLeft: session.timeLeft,
          targetScore: session.targetScore,
          gameStatus: session.timeOutMessage || "",
        };

        // 3. Emit the CLEAN object
        socket.emit("update-game-state", cleanSession);

        // console.log(`✅ Cleaned state sent to user ${userId}`);
      }
    });
  });

  socket.on("player-hold", async ({ roomId }) => {
    // Added async
    const session = gameSessions[roomId];
    if (!session) return;

    const activePlayerKey = session.player1.active ? "player1" : "player2";

    // 1. Update the score
    session[activePlayerKey].score += session.currentTurnScore;
    session.currentTurnScore = 0;

    // 2. WIN CONDITION CHECK
    if (session[activePlayerKey].score >= session.targetScore) {
      const winner = session[activePlayerKey];
      const opponentKey = activePlayerKey === "player1" ? "player2" : "player1";
      const loser = session[opponentKey];

      try {
        // Use the ID property as defined in your POJO (likely id or userId)
        await Promise.all([
          User.findByIdAndUpdate(
            winner.id,
            { $inc: { wins: 1 } },
            { new: true, runValidators: true },
          ),
          User.findByIdAndUpdate(
            loser.id,
            { $inc: { losses: 1 } },
            { new: true, runValidators: true },
          ),
        ]);
      } catch (err) {
        // console.error("DB Update Error:", err);
      }

      return io.to(roomId).emit("game-over", {
        session, // You might want to use cleanSession here too!
        winner: winner,
      });
    }

    // 3. SWAP TURNS (Only runs if no winner)
    if (activePlayerKey === "player1") {
      session.player1.active = false;
      session.player2.active = true;
    } else {
      session.player2.active = false;
      session.player1.active = true;
    }

    // 4. BROADCAST UPDATED STATE
    io.to(roomId).emit("update-game-state", session);
  });

  //User roll dice, the server only talks to that specifci room
  socket.on("roll-dice", (roomId) => {
    const result = Math.floor(Math.random() * 6) + 1;
    const session = gameSessions[roomId];
    session.stealHappenned = false;

    if (!session) return;

    if (result === 1) {
      session.currentTurnScore = 0;
      // Swap turn
      const p1WasActive = session.player1.active;
      session.player1.active = !p1WasActive;
      session.player2.active = p1WasActive;

      io.to(roomId).emit("dice-result", {
        result,
        turnSwapped: true,
        session, // Opponent sees turn move
        currentTurnScore: session.currentTurnScore,
      });
    } else if (result === 6) {
      if (session.player1.active && session.player2.score >= 6) {
        session.player1.score = session.player1.score + 6;
        session.player2.score = session.player2.score - 6;
        session.stealHappenned = true;
      }
      if (session.player2.active && session.player1.score >= 6) {
        session.player2.score = session.player2.score + 6;
        session.player1.score = session.player1.score - 6;
        session.stealHappenned = true;
      }

      io.to(roomId).emit("dice-result", {
        result,
        turnSwapped: false,
        session: { ...session },
        currentTurnScore: session.currentTurnScore,
      });
    } else {
      session.currentTurnScore = (session.currentTurnScore || 0) + result;
      // Just send the result
      io.to(roomId).emit("dice-result", {
        result,
        turnSwapped: false,
        session, // Opponent sees turn move
        currentTurnScore: session.currentTurnScore,
      });
    }

    io.to(roomId).emit("update-game-state", session);
  });

  // Let everyone know who's online
  io.emit("get-online-users", Object.keys(userSocketMap));

  // Handle Game Challenge
  socket.on("send-challenge", (data) => {
    const { sender, receiverId, gameName, gamePath } = data;

    // Is the opponent in another session?
    const isReceiverInGame = Object.values(gameSessions).some(
      (session) =>
        String(session.player1?.id) === String(receiverId) ||
        String(session.player2?.id) === String(receiverId),
    );

    if (isReceiverInGame) {
      return socket.emit("challenge-error", {
        message: "User is already in a match! 🎮",
      });
    }

    const opponentSocketId = userSocketMap[receiverId];
    const challengeId = `game-${socket.id}-${receiverId}`;

    activeChallenge[challengeId] = {
      senderId: socket.id,
      opponentSocketId,
      status: "pending",
    };

    if (opponentSocketId) {
      // Send invitation to target opponent
      socket.to(receiverId).emit("receive-challenge", {
        senderId: socket.id,
        sender,
        challengeId,
        gameName,
        gamePath,
      });

      // --- ACKNOWLEDGE TO SENDER THAT IT SUCKED THROUGH SAFELY ---
      socket.emit("challenge-sent-success", { gameName });
    } else {
      socket.emit("challenge-error", { message: "User is currently offline." });
    }
  });

  socket.on("accept-challenge", async (data) => {
    const { challengeId, receiverId, sender, gamePath } = data;

    const challenge = activeChallenge[challengeId];

    if (challenge) {
      activeChallenge[challengeId].status = "fulfilled";
      const p1SocketId = challenge.senderId;
      const p2SocketId = challenge.opponentSocketId;

      const p1Socket = io.sockets.sockets.get(p1SocketId);
      const p2Socket = io.sockets.sockets.get(p2SocketId);

      if (p1Socket && p2Socket) {
        const receiver =
          await User.findById(receiverId).select("username avatar");

        p1Socket.join(challengeId);
        p2Socket.join(challengeId);

        // 1. Create the session with placeholder word data
        const newSession = {
          player1: {
            id: sender?._id,
            socketId: p1SocketId,
            username: sender?.username,
            avatar: sender?.avatar || "/default-dp.png",
            score: 0,
            active: true,
          },
          player2: {
            id: receiverId,
            socketId: p2SocketId,
            username: receiver?.username || "Guest",
            avatar: receiver?.avatar || "/default-dp.png",
            score: 0,
            active: false,
          },
          scrambledWord: "...", // Placeholder so frontend knows it's loading
          timeLeft: 62,
          isLoading: false,
          timerId: null,
          targetScore: gamePath === "words-strike" ? 10 : 60,
        };

        // 2. Save to global state IMMEDIATELY
        gameSessions[challengeId] = newSession;

        // 3. Emit roles and redirect IMMEDIATELY
        p1Socket.emit("assign-role", "player1");
        p2Socket.emit("assign-role", "player2");

        // THE FIX: Explicitly pick only the safe fields to send to the UI
        const publicSession = {
          player1: newSession.player1,
          player2: newSession.player2,
          scrambledWord: newSession.scrambledWord,
          timeLeft: newSession.timeLeft,
          targetScore: newSession.targetScore,
        };

        io.to(challengeId).emit("start-game", {
          roomId: challengeId,
          session: publicSession,
          gamePath,
        });

        // console.log(gameSessions[challengeId]);
      }
    } else {
      socket.emit("error-message", {
        message: "Matchmaking failed: Player offline.",
      });
      delete activeChallenge[challengeId];
    }
  });

  ///////////////////////// READY LISTENER //////////////////////
  socket.on("player-ready-in-arena", async (roomId) => {
    const session = gameSessions[roomId];
    if (!session) return;

    // 1. Identify and mark the player as ready
    const playerRole =
      session.player1.socketId === socket.id ? "player1" : "player2";
    session[playerRole].ready = true;

    // console.log(`Arena: ${playerRole} in room ${roomId} is ready.`);

    // 2. Wait for BOTH players to be ready
    if (session.player1.ready && session.player2.ready) {
      // Safety: Prevent multiple intervals if the event is somehow fired twice
      if (session.timerId) return;

      try {
        const { original, scrambled } = await getNewChallenge();

        session.currentWord = original.toLowerCase();
        session.scrambledWord = scrambled.toLowerCase();
        session.timeLeft = 45; // Standardize to 45 for the game start

        // 3. Start the server-side interval
        startNeonTimer(roomId, io, gameSessions);

        // 4. Send word to clients
        io.to(roomId).emit("word-ready", {
          scrambledWord: session.scrambledWord,
          timeLeft: 45,
        });
      } catch (err) {
        // console.error("CRITICAL: Arena Fetch Failed:", err);

        // Notify clients of the failure
        io.to(roomId).emit("error-message", {
          message: "Arena Error: Could not load challenge.",
        });

        // 5. CLEANUP (Using roomId, NOT challengeId)
        if (session.timerId) clearInterval(session.timerId);

        // Kick both players back to dashboard
        io.to(roomId).emit("opponent-left", {
          message: "Game cancelled due to server error.",
        });

        // Cleanup global objects
        delete gameSessions[roomId];

        delete activeChallenge[roomId];
      }
    }
  });

  //////////////////////////////////////// SUBMIT LISTENER////////////////////////////////
  // 1. Make the handler 'async' so we can await the word fetch
  socket.on("submit-word", async ({ roomId, word, playerRole }) => {
    const session = gameSessions[roomId];
    if (!session || !word) return;

    const player = session[playerRole];

    io.to(roomId).emit("guessed-value", {
      word,
      player,
      playerRole,
    });

    const isCorrect = session.currentWord.toLowerCase() === word.toLowerCase();

    if (isCorrect && session.timerId) {
      // 1. CLEAR THE EXISTING TIMER IMMEDIATELY

      clearInterval(session.timerId);

      session.timeLeft = 45;

      // 2. UPDATE SERVER STATE
      session[playerRole].score += 10;

      // 3. EMIT DEDICATED SCORE EVENT (Lightweight & Immediate)
      // We call this "correct-guess" to separate it from "new-round"
      io.to(roomId).emit("correct-guess", {
        scoringPlayer: playerRole, // e.g., 'player1'
        player1Score: session.player1.score,
        player2Score: session.player2.score,
      });

      //Start Game Timer
      startNeonTimer(roomId, io, gameSessions);

      if (session[playerRole].score === session.targetScore) {
        // Define who won and who lost
        const winnerId = session[playerRole].id;
        const opponentRole = playerRole === "player1" ? "player2" : "player1";
        const loserId = session[opponentRole].id;

        // Perform DB Updates (Async - don't forget to handle these)
        try {
          // Increment Winner's wins
          await User.findByIdAndUpdate(winnerId, { $inc: { wins: 1 } });

          // Increment Loser's losses (since you just added that property!)
          await User.findByIdAndUpdate(loserId, { $inc: { losses: 1 } });
        } catch (err) {
          // console.error("Failed to update game stats:", err);
        }

        // Emit the final game over event
        return io.to(roomId).emit("game-over", {
          winner: session[playerRole],
          message: "🏆 Match Point! Game Over.",
        });
      }

      // 5. NEXT ROUND LOGIC (Separated)
      try {
        const { original, scrambled } = await getNewChallenge();
        session.currentWord = original.toLowerCase();
        session.scrambledWord = scrambled.toLowerCase();

        // Emit new-round ONLY for the word/timer change
        io.to(roomId).emit("new-round", {
          scrambledWord: session.scrambledWord,
          timeLeft: 45,
          message: "🎯 Next Word...",
        });
      } catch (err) {
        io.to(roomId).emit("error-message", { message: "Word API Error." });
      }
    } else {
      socket.emit("guess-wrong", { message: "❌ Try again!" });
    }
  });

  /////////////////////////////////////////////////////////////////////////////////////

  socket.on("leave-game", (roomId) => {
    if (roomId && gameSessions[roomId]) {
      const session = gameSessions[roomId];

      // 1. STOP THE CLOCK
      if (session.timerId) {
        clearInterval(session.timerId);
        // console.log(`⏱️ Timer cleared for room: ${roomId}`);
      }

      // 2. Notify opponent and clean up
      socket.to(roomId).emit("opponent-left");
      delete gameSessions[roomId];
      socket.leave(roomId);
    }
  });
  //Handle Game Challenge Rejection
  socket.on("reject-challenge", (data) => {
    const { receiverId } = data;
    const receiverSocketId = userSocketMap[receiverId];
    // Tell the original sender the bad news
    io.to(receiverSocketId).emit("challenge-declined", {
      message: `Challenge Declined`,
    });
  });

  /////////////// Messages Status Fucntionality ////////////
  socket.on("send-message", async (message) => {
    const { _id, receiverId, senderId } = message;
    const receiverSocketId = userSocketMap[String(receiverId)];

    // Fetch sender data once
    const senderData = await User.findById(senderId).select("-password -__v");

    if (receiverSocketId) {
      // Forward message WITH sender profile attached
      socket.to(receiverSocketId).emit("receive-message", {
        ...message,
        senderData, // ← receiver can now add sender to their chatUsers
      });

      socket.emit("message-delivered-update", {
        messageId: _id,
        status: "delivered",
      });
      await Message.findByIdAndUpdate(
        _id,
        { status: "delivered" },
        { new: true },
      );
    } else {
      socket.emit("message-status-update", {
        messageId: _id,
        status: "sent",
      });
      await Message.findByIdAndUpdate(_id, { status: "sent" }, { new: true });
    }
  });

  /////////////////////////TYPING DETAILS//////////////////////////////
  socket.on("typing", ({ receiverId, isTyping }) => {
    socket
      .to(receiverId)
      .emit("sender-typing-status", { isTyping, senderId: socket.userId });
  });

  socket.on("mark-read", async ({ messageId, senderId }) => {
    if (!mongoose.Types.ObjectId.isValid(messageId)) return;
    // Find the socket of the person who SENT the message
    const senderSocketId = userSocketMap[senderId];

    if (senderSocketId) {
      // Send the update specifically to the sender
      io.to(senderSocketId).emit("message-read-update", {
        messageId,
        status: "read",
      });
    }

    //  Update the status in your MongoDB here too
    await Message.findByIdAndUpdate(
      messageId,
      { status: "read" },
      { new: true },
    );
  });

  /////////////////////////// DELETE MESSAGE ////////////////////////////////////////////
  socket.on("message-deleted", ({ messageId, receiverId }) => {
    const senderId = socket.userId;
    const receiverSocketId = userSocketMap[receiverId];
    const senderSocketId = userSocketMap[senderId];

    if (receiverSocketId) {
      io.to(receiverSocketId).emit("delete-both-message", messageId);
    }

    if (senderSocketId) {
      io.to(senderSocketId).emit("delete-both-message", messageId);
    }
  });

  // Handle the exit (Correct keyword + Correct syntax)
  socket.on("disconnect", () => {
    const userId = Object.keys(userSocketMap).find(
      (key) => userSocketMap[key] === socket.id,
    );

    if (userId) {
      delete userSocketMap[userId];
      // console.log(`🔌 User ${userId} disconnected.`);

      io.emit("update-online-users", Object.keys(userSocketMap));
    }

    // Iterate through sessions to find if this user was in a game
    Object.keys(gameSessions).forEach((roomId) => {
      const session = gameSessions[roomId];

      if (session.player1.id === userId || session.player2.id === userId) {
        // 1. Notify the opponent immediately
        io.to(roomId).emit("opponent-disconnected", {
          message: "Opponent connection lost. Waiting 10s...",
          isReconnecting: true,
        });

        // 2. Start the Grace Period
        setTimeout(() => {
          // IMPORTANT: We check the current state of the map NOW (after 10s)
          const newSocketId = userSocketMap[userId];

          if (!newSocketId) {
            // Notify remaining player to exit
            io.to(roomId).emit("opponent-left", {
              message: "Opponent timed out. Game ended.",
            });

            // Physical cleanup of the room
            const room = io.sockets.adapter.rooms.get(roomId);
            if (room) {
              room.forEach((sId) => {
                const s = io.sockets.sockets.get(sId);
                if (s) s.leave(roomId);
              });
            }

            // 1. STOP THE CLOCK
            if (session.timerId) {
              clearInterval(session.timerId);
              // console.log(`⏱️ Timer cleared for room: ${roomId}`);
            }

            // THE MISSING LINE: Actually remove it from server memory
            delete gameSessions[roomId];
          } else {
            // Update the session with the new socket ID
            if (session.player1.id === userId) {
              session.player1.socketId = newSocketId;
            } else {
              session.player2.socketId = newSocketId;
            }

            const cleanSession = {
              player1: {
                id: String(session.player1.id),
                username: session.player1.username,
                avatar: session.player1.avatar,
                score: session.player1.score,
              },
              player2: {
                id: String(session.player2.id),
                username: session.player2.username,
                avatar: session.player2.avatar,
                score: session.player2.score,
              },
              scrambledWord: session.scrambledWord,
              timeLeft: session.timeLeft,
              gameStatus: "User Reconnected!",
            };

            // Sync everyone up safely
            io.to(roomId).emit("update-game-state", cleanSession);
          }
        }, 15000); // Give them a full 10 seconds
      }
    });
  });
});

const PORT = process.env.PORT || 5000;

const startServer = async function () {
  try {
    await connectDB();

    server.listen(PORT, function () {
      console.log("listening...");
    });
  } catch (err) {
    // console.log("failed to start server", err.message);
    process.exit(1);
  }
};

// Mounting the routers
app.use("/api/smile/v1/users", userRouter);
app.use("/api/smile/v1/messages", messageRouter);
app.use("/api/smile/v1/users/connections", connectionRouter);

// Catch undefined routes
app.use(function (req, res) {
  res.status(404).json({
    status: "fail",
    message: `No route found for the path ${req.originalUrl}`,
  });
});

// Global error handling
app.use(globalErrorHandler);
startServer();
