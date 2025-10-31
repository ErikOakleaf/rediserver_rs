use crate::connection::BUFFER_SIZE;
use crate::error::ConnectionError;

pub struct WriteState {
    pub buffer: [u8; BUFFER_SIZE],
    pub size: usize,
    pub bytes_written: usize,
}

impl WriteState {
    pub fn new() -> Self {
        WriteState {
            buffer: [0u8; BUFFER_SIZE],
            size: 0,
            bytes_written: 0,
        }
    }

    pub fn append_bytes(&mut self, status: u32, data: &[u8]) -> Result<(), ConnectionError> {
        const STATUS_LEN: usize = 4;
        const LENGTH_LEN: usize = 4;

        let total_len = LENGTH_LEN + STATUS_LEN + data.len();

        if self.size + total_len > BUFFER_SIZE {
            return Err(ConnectionError::WriteBufferOverflow);
        }

        let start = self.size;

        // length field
        let len_bytes = ((STATUS_LEN + data.len()) as u32).to_be_bytes();
        self.buffer[start..start + LENGTH_LEN].copy_from_slice(&len_bytes);

        // status field
        let status_bytes = status.to_be_bytes();
        let status_start = start + LENGTH_LEN;
        self.buffer[status_start..status_start + STATUS_LEN].copy_from_slice(&status_bytes);

        // data bytes
        let data_start = status_start + STATUS_LEN;
        self.buffer[data_start..data_start + data.len()].copy_from_slice(data);

        self.size += total_len;

        Ok(())
    }

    pub fn append_amount_responses_header(
        &mut self,
        amount_responses: u32,
    ) -> Result<(), ConnectionError> {
        const AMOUNT_BYTES: usize = 4;

        if self.size + AMOUNT_BYTES > BUFFER_SIZE {
            return Err(ConnectionError::WriteBufferOverflow);
        }

        let amount_responses_bytes = amount_responses.to_be_bytes();
        self.buffer[self.size..self.size + AMOUNT_BYTES].copy_from_slice(&amount_responses_bytes);
        self.size += AMOUNT_BYTES;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_bytes() {
        struct Message {
            status: u32,
            data: &'static [u8],
        }

        struct TestData {
            messages: Vec<Message>,
            expected_size: usize,
            expected_buffer: &'static [u8],
        }

        let tests = vec![
            TestData {
                messages: vec![Message {
                    status: 0,
                    data: &[],
                }],
                expected_size: 8,
                expected_buffer: &[0, 0, 0, 4, 0, 0, 0, 0],
            },
            TestData {
                messages: vec![Message {
                    status: 0,
                    data: &[5, 5, 5, 5, 5, 5, 5, 5],
                }],
                expected_size: 16,
                expected_buffer: &[0, 0, 0, 12, 0, 0, 0, 0, 5, 5, 5, 5, 5, 5, 5, 5],
            },
            TestData {
                messages: vec![
                    Message {
                        status: 0,
                        data: &[],
                    },
                    Message {
                        status: 1,
                        data: &[],
                    },
                    Message {
                        status: 2,
                        data: &[],
                    },
                ],
                expected_size: 24,
                expected_buffer: &[
                    0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 2,
                ],
            },
            TestData {
                messages: vec![
                    Message {
                        status: 0,
                        data: &[5, 5],
                    },
                    Message {
                        status: 1,
                        data: &[5, 5, 5],
                    },
                    Message {
                        status: 2,
                        data: &[5, 5, 5, 5],
                    },
                ],
                expected_size: 33,
                expected_buffer: &[
                    0, 0, 0, 6, 0, 0, 0, 0, 5, 5, 0, 0, 0, 7, 0, 0, 0, 1, 5, 5, 5, 0, 0, 0, 8, 0,
                    0, 0, 2, 5, 5, 5, 5,
                ],
            },
        ];

        for test in tests {
            let mut write_state = WriteState::new();

            for message in test.messages {
                write_state
                    .append_bytes(message.status, message.data)
                    .unwrap();
            }

            assert_eq!(
                test.expected_size, write_state.size,
                "expected size: {}\ngot {}",
                test.expected_size, write_state.size
            );

            assert_eq!(
                test.expected_buffer,
                &write_state.buffer[..write_state.size],
                "expected buffer: {:?}\ngot {:?}",
                &test.expected_buffer,
                &write_state.buffer[..write_state.size]
            );
        }
    }
}
