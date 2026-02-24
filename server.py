import os
import time
import select
import argparse
import traceback
import threading
import queue
from pathlib import Path
from PIL import Image

def process_image_to_bw(input_path: str) -> str:
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Processing: {input_path}")    
    p = Path(input_path)
    output_path = str(p.parent / f"{p.stem}_bw{p.suffix}")
    
    time.sleep(10)

    try:
        image = Image.open(input_path).convert("RGB")
        width, height = image.size
        pixels = image.load()

        for y in range(height):
            for x in range(width):
                r, g, b = pixels[x, y]
                gray = int(0.299 * r + 0.587 * g + 0.114 * b)
                pixels[x, y] = (gray, gray, gray)
        
        image.save(output_path)
        
        print(f"[{thread_name}] Processing completed. Result saved in: {output_path}")
        return output_path

    except FileNotFoundError:
        print(f"[{thread_name}] Error with path {input_path}")
        raise
    except Exception as e:
        print(f"[{thread_name}] Error in conversion func: {e}")
        raise

class Server:
    def __init__(self, channel_name: str, num_workers: int = 3):
        self.request_pipe_path = f"/tmp/{channel_name}.req.pipe"
        self.num_workers = num_workers
        self.task_queue = queue.Queue()
        self.workers = []
        self._req_fd = None

    def _consumer_worker(self):
        while True:
            try:
                task = self.task_queue.get()

                if task is None:
                    print(f"[{threading.current_thread().name}] Stop.")
                    break

                image_path, response_pipe_path = task
                thread_name = threading.current_thread().name
                print(f"[{thread_name}] picked: '{image_path}'")

                try:
                    result_path = process_image_to_bw(image_path)
                    response_text = f"Success:{result_path}"
                except Exception as e:
                    print(f"[{thread_name}] Error '{image_path}': {e}")
                    response_text = f"Error: {os.path.basename(image_path)}"

                try:
                    resp_fd = os.open(response_pipe_path, os.O_WRONLY)
                    os.write(resp_fd, response_text.encode('utf-8'))
                    os.close(resp_fd)
                except OSError as e:
                    print(f"[{thread_name}] Failed to open channel '{response_pipe_path}': {e}")
                    pass

            except Exception as e:
                print(f"[{threading.current_thread().name}] Error: {e}")
                traceback.print_exc()
            finally:
                self.task_queue.task_done()

    def start(self):
        print(f"Server up with {self.num_workers} workers.")
        print(f"Channel: {self.request_pipe_path}")

        if os.path.exists(self.request_pipe_path):
            os.unlink(self.request_pipe_path)
        os.mkfifo(self.request_pipe_path)

        for i in range(self.num_workers):
            thread = threading.Thread(target=self._consumer_worker, name=f"Worker-{i + 1}")
            thread.start()
            self.workers.append(thread)

        try:
            self._req_fd = os.open(self.request_pipe_path, os.O_RDONLY)
            print("Channel open...")

            while True:
                request_bytes = os.read(self._req_fd, 4096)

                if not request_bytes:
                    print("Channel closed. Restart...")
                    os.close(self._req_fd)
                    self._req_fd = os.open(self.request_pipe_path, os.O_RDONLY)
                    continue

                try:
                    request_str = request_bytes.decode('utf-8')
                    # "путь_к_файлу:путь_к_каналу_ответа"
                    image_path, response_pipe_path = request_str.strip().split(":", 1)

                    print(f"[Producer] Make: '{image_path}'. Added to the queue.")
                    task = (image_path, response_pipe_path)
                    self.task_queue.put(task)

                except (UnicodeDecodeError, ValueError) as e:
                    print(f"[Producer] Error: {e}")
                    continue

        except KeyboardInterrupt:
            print("\nEnd.")
        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
        finally:
            self._shutdown()

    def _shutdown(self):
        print("Ending...")

        for _ in self.workers:
            self.task_queue.put(None)

        print("Ending tasks...")
        self.task_queue.join()

        for worker in self.workers:
            worker.join()

        if self._req_fd is not None:
            os.close(self._req_fd)
        if os.path.exists(self.request_pipe_path):
            os.unlink(self.request_pipe_path)

        print("End.")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("channel", nargs="?", default="image_proc_channel", help="Name of main channel.")
    p.add_argument("-w", "--workers", type=int, default=3, help="Number of active threads.")
    args = p.parse_args()
    return args.channel, args.workers


if __name__ == "__main__":
    channel_name, num_workers = parse_args()
    server = Server(channel_name=channel_name, num_workers=num_workers)
    server.start()
