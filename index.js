import mongoose from "mongoose";
import dotenv from "dotenv";
import amqp from "amqplib/callback_api.js";
import {
  compile_cpp,
  compile_cpp_playground,
} from "./controllers/compilationController.js";
import { Submission } from "./models/Submission.js";
import { Playground_Submission } from "./models/Playground_Submission.js";

dotenv.config();

const supported_langaues = ["cpp", "java", "python", "js"];
let rabbitMQ_channel;

const compile = async (submission_id, type) => {
  if (type === "problem_submission" || type == "contest_submission") {
    let submission = await Submission.findById(submission_id);
    if (!submission) {
      console.log("invalid submission");
      return;
    }

    if (!supported_langaues.includes(submission.language)) {
      console.log(`language ${submission.language} not supported`);
      return;
    }

    if (submission.language === "cpp") {
      submission = await compile_cpp(submission, type);
      console.log(submission);
    }

    rabbitMQ_channel.sendToQueue(
      "processed_submission",
      Buffer.from(
        JSON.stringify({
          submission: submission,
          type: type,
        })
      )
    );
    return;
  }

  let playground_submission = await Playground_Submission.findById(
    submission_id
  );

  if (!playground_submission) {
    console.log("invalid playground submission");
    return;
  }

  if (!supported_langaues.includes(playground_submission.language)) {
    console.log(`language ${playground_submission.language}  not supported`);
    return;
  }

  if (playground_submission.language === "cpp") {
    playground_submission = await compile_cpp_playground(playground_submission);
  }

  rabbitMQ_channel.sendToQueue(
    "processed_submission",
    Buffer.from(
      JSON.stringify({
        submission: playground_submission,
        type: type,
      })
    )
  );
  return;
};

const connect_to_mongoDB = async () => {
  try {
    mongoose.connect(process.env.MONGODB_URI);
    console.log("Connected to mongoDB database");
  } catch (err) {
    throw err;
  }
};

const connect_to_rabbitMQ = async () => {
  try {
    amqp.connect(
      process.env.RABBIT_MQ_URI,
      {
        heartbeat: 30,
      },
      function (error, connection) {
        if (error) {
          throw error;
        }

        connection.createChannel(function (error, channel) {
          if (error) {
            throw error;
          }

          channel.assertQueue("submission_requests", {
            durable: false,
          });
          console.log("waiting for messages...");

          channel.consume(
            "submission_requests",
            function (msg) {
              const submission_data = JSON.parse(msg.content.toString());

              compile(submission_data.submission_id, submission_data.type);
            },
            {
              noAck: true,
            }
          );

          channel.assertQueue("processed_submission", {
            durable: false,
          });

          rabbitMQ_channel = channel;
        });
      }
    );
    console.log("Connected to rabbitMQ");
  } catch (err) {
    console.log(err);
  }
};

const startup = async () => {
  try {
    await connect_to_mongoDB();
    await connect_to_rabbitMQ();
  } catch (err) {
    console.log(err);
  }
};

startup();
