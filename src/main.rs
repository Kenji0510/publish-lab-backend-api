use std::sync::mpsc;

use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, FieldTable, QueueDeclareOptions, Result,
};
use axum::{
    Router,
    extract::{
        WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::from_str;
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};

#[derive(Deserialize, Debug)]
struct MQData {
    num_points: usize,
    color: String,
    points: Vec<f64>,
}

fn get_data_from_rabbitmq(
    tx: mpsc::SyncSender<String>,
    stop_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut connection = Connection::insecure_open("amqp://user:password@192.168.100.200")?;
    // .expect("Failed to connect to RabbitMQ");
    let channel = connection.open_channel(None)?;
    // .expect("Failed to open a channel");

    let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
    // .expect("Failed to declare a queue");

    channel.queue_bind("hello", "amq.direct", "hello", FieldTable::default())?;

    let consumer = queue.consume(ConsumerOptions::default())?;
    println!("--> {:12} - Starting to listen to RabbitMQ", "LOGGER");

    for (i, message) in consumer.receiver().iter().enumerate() {
        if stop_rx.has_changed().unwrap_or(false) {
            println!("Received stop signal");
            break;
        }

        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                println!("({:>3}) Received ", i);
                // println!("--> {:12} - Received data size: {}", "LOGGER", body.len());
                if tx.send(body.to_string()).is_err() {
                    println!("Failed to send data to websocket");
                    // break;
                }
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }

    println!("--> {:12} - Closing connection to RabbitMQ", "LOGGER");

    connection.close()?;
    Ok(())
}

async fn handle_ws(ws: WebSocketUpgrade) -> impl IntoResponse {
    println!("--> {:12} - Accessed /ws", "HANDLER");
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(socket: WebSocket) {
    println!("--> {:12} - Connected to websocket", "LOGGER");

    let (mut sender, mut receiver) = socket.split();

    let (tx, rx) = mpsc::sync_channel::<String>(5);
    let (stop_tx, stop_rx) = watch::channel(());
    let stop_rx_for_rabbit = stop_rx.clone();
    let stop_rx_for_send = stop_rx.clone();

    let rabbitmq_task = tokio::task::spawn_blocking(move || {
        if let Err(e) = get_data_from_rabbitmq(tx, stop_rx_for_rabbit) {
            eprintln!("Error in RabbitMQ thread: {:?}", e);
        }
    });

    let send_task = tokio::spawn(async move {
        while let Ok(message) = rx.recv() {
            if stop_rx_for_send.has_changed().unwrap_or(false) {
                println!("--> {:12} - Detected stop signal in send_task", "LOGGER");
                break;
            }

            let mut data_with_color = vec![];
            // Test
            match from_str::<MQData>(&message) {
                Ok(mq_data) => {
                    println!("--> {:12} - Deserialized data from RabbitMQ", "LOGGER");
                    println!("Num points: {:?}", mq_data.num_points);
                    println!("Data: {:?}", mq_data.points[0]);

                    let points_vec = &mq_data.points;

                    for chunk in points_vec.chunks(3) {
                        if chunk.len() == 3 {
                            data_with_color.extend_from_slice(chunk);
                            data_with_color.extend_from_slice(&[0.0, 1.0, 0.0]);
                        }
                    }
                }
                Err(e) => {
                    println!(
                        "--> {:12} - Failed to deserialize data from RabbitMQ",
                        "LOGGER"
                    );
                    println!("Error: {:?}", e);
                }
            }
            // if sender.send(Message::Text(message)).await.is_err() {
            //     println!("--> {:12} - Failed to send message to client", "LOGGER");
            //     break;
            // }
            if sender
                .send(Message::Binary(
                    bytemuck::cast_slice(&data_with_color).to_vec(),
                ))
                .await
                .is_err()
            {
                println!("--> {:12} - Failed to send binary data", "LOGGER");
            }
        }
    });
    loop {
        match receiver.next().await {
            Some(Ok(msg)) => match msg {
                axum::extract::ws::Message::Text(req_data) => {
                    println!("--> {:12} - Received data from client", "LOGGER");
                    println!("Data: {}", req_data);
                }
                axum::extract::ws::Message::Close(_) => {
                    println!("--> {:12} - Closing websocket connection", "LOGGER");
                    break;
                }
                _ => {}
            },
            Some(Err(e)) => {
                println!("--> {:12} - Websocket error: {:?}", "LOGGER", e);
                break;
            }
            None => {
                println!("--> {:12} - Websocket stream closed (None)", "LOGGER");
                break;
            }
        }
    }

    let _ = stop_tx.send(());

    send_task.await.unwrap();
    rabbitmq_task.await.unwrap();
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any);

    let app = Router::new().route("/ws", get(handle_ws)).layer(cors);

    println!("--> {:12} - started running server on port 8080", "INFO");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
