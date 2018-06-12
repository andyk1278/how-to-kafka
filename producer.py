# producer.py

import time
import cv2
from kafka import KafkaProducer, KafkaClient


# connect Kafka
kafka = KafkaClient('localhost:9092')

producer = KafkaProducer(kafka)

# assign a topic
topic = 'my-topic'

def video_emitter(video):
    # Open the video
    video = cv2.VideoCapture(video)
    print(' emitting.....')

    # read the file
    while (video.isOpened):
        # read images in each frame
        success, image = video.read()

        # check if the file has reached EOF
        if not success:
            break

        # convert the image to a PNG file
        ret, jpeg = cv2.imencode('.png', image)

        # convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())

        # To reduce CPU usage, using sleep time of 0.2 sec
        time.sleep(0.2)

    # clear the capture
    video.release()
    print('done emitting.')


if __name__ == '__main__':
    video_emitter('video.mp4')
