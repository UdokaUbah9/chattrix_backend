require("dotenv").config();
const express = require("express");
const { Server } = require("socket.io");
const http = require("http");
const cors = require("cors");
const cookieParser = require("cookie-parser");
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

const helmet = require("helmet");
const mongoSanitize = require("express-mongo-sanitize");
const hpp = require("hpp");

const app = express();

const allowedOrigins = ["http://localhost:3000", process.env.FRONTEND_URL];

const server = http.createServer(app);

app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    contentSecurityPolicy: false,
  }),
);

app.use(
  cors({
    origin: allowedOrigins,
    credentials: true,
  }),
);

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));
app.use(hpp());
app.use(cookieParser());

const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true,
  },
  pingTimeout: 60000,
  pingInterval: 25000,
});

const userSocketMap = {};
const gameSessions = {};
const activeChallenge = {};
// ⚡ Tracks our 5-second countdown instances per room
const disconnectTimers = new Map();

app.set("socketio", io);
app.set("userSocketMap", userSocketMap);

io.on("connection", (socket) => {
  socket.on("identify-user", async (userId) => {
    if (userId) {
      socket.userId = userId;
      userSocketMap[userId] = socket.id;
      socket.join(userId);

      io.emit("update-online-users", Object.keys(userSocketMap));

      try {
        const undeliveredMessages = await Message.find({
          receiverId: userId,
          status: "sent",
        });

        if (undeliveredMessages.length > 0) {
          await Message.updateMany(
            { _id: { $in: undeliveredMessages.map((m) => m._id) } },
            { $set: { status: "delivered" } },
          );

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
        // Handle error silently
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
        // ⚡ CRITICAL: Sabotage and kill the 5s disconnect timer if the player returns in time!
        if (disconnectTimers.has(roomId)) {
          clearTimeout(disconnectTimers.get(roomId));
          disconnectTimers.delete(roomId);
          io.to(roomId).emit("opponent-reconnected", { userId });
        }

        socket.join(roomId);

        if (String(session.player1.id) === String(userId)) {
          session.player1.socketId = socket.id;
        } else if (String(session.player2.id) === String(userId)) {
          session.player2.socketId = socket.id;
        }

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

        socket.emit("update-game-state", cleanSession);
      }
    });
  });

  socket.on("player-hold", async ({ roomId }) => {
    const session = gameSessions[roomId];
    if (!session) return;

    const activePlayerKey = session.player1.active ? "player1" : "player2";

    session[activePlayerKey].score += session.currentTurnScore;
    session.currentTurnScore = 0;

    if (session[activePlayerKey].score >= session.targetScore) {
      const winner = session[activePlayerKey];
      const opponentKey = activePlayerKey === "player1" ? "player2" : "player1";
      const loser = session[opponentKey];

      try {
        await Promise.all([
          User.findByIdAndUpdate(winner.id, { $inc: { wins: 1 } }),
          User.findByIdAndUpdate(loser.id, { $inc: { losses: 1 } }),
        ]);
      } catch (err) {
        // Handled silently
      }

      return io.to(roomId).emit("game-over", {
        session,
        winner: winner,
      });
    }

    if (activePlayerKey === "player1") {
      session.player1.active = false;
      session.player2.active = true;
    } else {
      session.player2.active = false;
      session.player1.active = true;
    }

    io.to(roomId).emit("update-game-state", session);
  });

  socket.on("roll-dice", (roomId) => {
    const result = Math.floor(Math.random() * 6) + 1;
    const session = gameSessions[roomId];
    if (!session) return;

    session.stealHappenned = false;

    if (result === 1) {
      session.currentTurnScore = 0;
      const p1WasActive = session.player1.active;
      session.player1.active = !p1WasActive;
      session.player2.active = p1WasActive;

      io.to(roomId).emit("dice-result", {
        result,
        turnSwapped: true,
        session,
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
      io.to(roomId).emit("dice-result", {
        result,
        turnSwapped: false,
        session,
        currentTurnScore: session.currentTurnScore,
      });
    }

    io.to(roomId).emit("update-game-state", session);
  });

  io.emit("get-online-users", Object.keys(userSocketMap));

  socket.on("send-challenge", (data) => {
    const { sender, receiverId, gameName, gamePath } = data;

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
      socket.to(receiverId).emit("receive-challenge", {
        senderId: socket.id,
        sender,
        challengeId,
        gameName,
        gamePath,
      });
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
          scrambledWord: "...",
          timeLeft: 62,
          isLoading: false,
          timerId: null,
          targetScore: gamePath === "words-strike" ? 100 : 60,
        };

        gameSessions[challengeId] = newSession;

        p1Socket.emit("assign-role", "player1");
        p2Socket.emit("assign-role", "player2");

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
      }
    } else {
      socket.emit("error-message", {
        message: "Matchmaking failed: Player offline.",
      });
      delete activeChallenge[challengeId];
    }
  });

  socket.on("player-ready-in-arena", async (roomId) => {
    const session = gameSessions[roomId];
    if (!session) return;

    const playerRole =
      session.player1.socketId === socket.id ? "player1" : "player2";
    session[playerRole].ready = true;

    if (session.player1.ready && session.player2.ready) {
      if (session.timerId) return;

      try {
        const { original, scrambled } = await getNewChallenge();

        session.currentWord = original.toLowerCase();
        session.scrambledWord = scrambled.toLowerCase();
        session.timeLeft = 45;

        startNeonTimer(roomId, io, gameSessions);

        io.to(roomId).emit("word-ready", {
          scrambledWord: session.scrambledWord,
          timeLeft: 45,
        });
      } catch (err) {
        if (session.timerId) clearInterval(session.timerId);

        io.to(roomId).emit("opponent-left", {
          message: "Game cancelled due to server error.",
        });

        delete gameSessions[roomId];
        delete activeChallenge[roomId];
      }
    }
  });

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
      clearInterval(session.timerId);
      session.timeLeft = 45;
      session[playerRole].score += 10;

      io.to(roomId).emit("correct-guess", {
        scoringPlayer: playerRole,
        player1Score: session.player1.score,
        player2Score: session.player2.score,
      });

      startNeonTimer(roomId, io, gameSessions);

      if (session[playerRole].score === session.targetScore) {
        const winnerId = session[playerRole].id;
        const opponentRole = playerRole === "player1" ? "player2" : "player1";
        const loserId = session[opponentRole].id;

        try {
          await User.findByIdAndUpdate(winnerId, { $inc: { wins: 1 } });
          await User.findByIdAndUpdate(loserId, { $inc: { losses: 1 } });
        } catch (err) {
          // Handled silently
        }

        return io.to(roomId).emit("game-over", {
          winner: session[playerRole],
          message: "🏆 Match Point! Game Over.",
        });
      }

      try {
        const { original, scrambled } = await getNewChallenge();
        session.currentWord = original.toLowerCase();
        session.scrambledWord = scrambled.toLowerCase();

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

  socket.on("leave-game", (roomId) => {
    if (roomId && gameSessions[roomId]) {
      const session = gameSessions[roomId];

      if (session.timerId) {
        clearInterval(session.timerId);
      }

      socket.to(roomId).emit("opponent-left");
      delete gameSessions[roomId];
      socket.leave(roomId);
    }
  });

  socket.on("reject-challenge", (data) => {
    const { receiverId } = data;
    const receiverSocketId = userSocketMap[receiverId];
    io.to(receiverSocketId).emit("challenge-declined", {
      message: `Challenge Declined`,
    });
  });

  socket.on("send-message", async (message) => {
    const { _id, receiverId, senderId } = message;
    const receiverSocketId = userSocketMap[String(receiverId)];

    const senderData = await User.findById(senderId).select("-password -__v");

    if (receiverSocketId) {
      socket.to(receiverSocketId).emit("receive-message", {
        ...message,
        senderData,
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

  socket.on("typing", ({ receiverId, isTyping }) => {
    socket
      .to(receiverId)
      .emit("sender-typing-status", { isTyping, senderId: socket.userId });
  });

  socket.on("mark-read", async ({ messageId, senderId }) => {
    if (!mongoose.Types.ObjectId.isValid(messageId)) return;
    const senderSocketId = userSocketMap[senderId];

    if (senderSocketId) {
      io.to(senderSocketId).emit("message-read-update", {
        messageId,
        status: "read",
      });
    }

    await Message.findByIdAndUpdate(
      messageId,
      { status: "read" },
      { new: true },
    );
  });

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

  // =============================================
  // ⚡ GENTLE 5-SECOND RECOVERY DISCONNECT SYSTEM
  // =============================================
  socket.on("disconnect", () => {
    const userId = Object.keys(userSocketMap).find(
      (key) => userSocketMap[key] === socket.id,
    );

    if (userId) {
      delete userSocketMap[userId];
      io.emit("update-online-users", Object.keys(userSocketMap));
    }

    // Scan for running game rooms that include the disconnected user
    Object.keys(gameSessions).forEach((roomId) => {
      const session = gameSessions[roomId];

      if (
        String(session.player1.id) === String(userId) ||
        String(session.player2.id) === String(userId)
      ) {
        // Broadcast to the other player that their opponent's connection staggered
        socket.to(roomId).emit("opponent-disconnected-grace", {
          userId,
          message: "Opponent disconnected. Waiting 5s for reconnection...",
        });

        // Set up our clean 5-second execution countdown
        const timeoutId = setTimeout(() => {
          console.log(
            `Grace period expired for room: ${roomId}. Executing session wipe.`,
          );

          if (session.timerId) {
            clearInterval(session.timerId);
          }

          io.to(roomId).emit("opponent-left", {
            message: "Opponent left or disconnected. Match terminated.",
          });

          const room = io.sockets.adapter.rooms.get(roomId);
          if (room) {
            room.forEach((sId) => {
              const s = io.sockets.sockets.get(sId);
              if (s) s.leave(roomId);
            });
          }

          delete gameSessions[roomId];
          disconnectTimers.delete(roomId);
        }, 5000); // 👈 Changed from 15s to exactly 5s grace countdown

        disconnectTimers.set(roomId, timeoutId);
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
    process.exit(1);
  }
};

app.use("/api/smile/v1/users", userRouter);
app.use("/api/smile/v1/messages", messageRouter);
app.use("/api/smile/v1/users/connections", connectionRouter);

app.use(function (req, res) {
  res.status(404).json({
    status: "fail",
    message: `No route found for the path ${req.originalUrl}`,
  });
});

app.use(globalErrorHandler);
startServer();
