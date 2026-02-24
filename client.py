import os
import argparse
import traceback
import select


class Client:
    def __init__(self, channel_name: str):
        self.request_pipe_path = f"/tmp/{channel_name}.req.pipe"
        self._counter = 0

    def run(self):
        print(f"Active channel: {self.request_pipe_path}")

        if not os.path.exists(self.request_pipe_path):
            print(f"Error: channel doesnt find: '{self.request_pipe_path}'. Check server")
            return

        try:
            while True:
                image_path_input = input("Enter the path to the image file (or 'exit' to exit): ").strip()

                if not image_path_input:
                    continue
                if image_path_input.lower() == 'exit':
                    break

                image_path = os.path.abspath(image_path_input)

                if not os.path.isfile(image_path):
                    print(f"Path error '{image_path}'")
                    continue

                self._counter += 1
                response_pipe_path = f"/tmp/resp_{os.getpid()}_{self._counter}.pipe"
                os.mkfifo(response_pipe_path)

                try:
                    message = f"{image_path}:{response_pipe_path}"

                    req_fd = os.open(self.request_pipe_path, os.O_WRONLY)
                    os.write(req_fd, message.encode('utf-8'))
                    os.close(req_fd)

                    print(f"Request '{os.path.basename(image_path)}' send.")

                    resp_fd = os.open(response_pipe_path, os.O_RDONLY)


                    if readable:
                        response_bytes = os.read(resp_fd, 4096)
                        response_str = response_bytes.decode('utf-8')
                        print(f"-> Response: {response_str}")

                    os.close(resp_fd)

                except Exception as e:
                    print(f"Error: {e}")
                    traceback.print_exc()
                finally:
                    if os.path.exists(response_pipe_path):
                        os.unlink(response_pipe_path)

        except KeyboardInterrupt:
            print("\nEnding...")
        finally:
            print("End.")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("channel", nargs="?", default="image_proc_channel", help="Name of channel.")
    args = p.parse_args()
    return args.channel


if __name__ == "__main__":
    channel = parse_args()
    client = Client(channel_name=channel)
    client.run()
