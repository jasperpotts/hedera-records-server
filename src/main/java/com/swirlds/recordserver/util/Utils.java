package com.swirlds.recordserver.util;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

public class Utils {
	public static final double TINY_BAR_IN_HBAR = 100_000_000;
	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss.nX");
	private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
	private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
	private static final BigInteger SECONDS_TO_NANOSECONDS = BigInteger.valueOf(1_000_000_000L);

	public static void failWithError(Throwable e) {
		e.printStackTrace();
		System.err.flush();
		System.exit(1);
	}

	public static String toHex(byte[] bytes) {
		byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}

	public static String toBase64(byte[] bytes) {
		return BASE64_ENCODER.encodeToString(bytes);
	}

	public static String toBinary(byte data) {
		return Integer.toBinaryString((data & 0xFF) + 0x100).substring(1);
	}

	public static String toBinary(int data) {
		return Integer.toBinaryString(data);
	}

	public static ByteBuffer readFileFully(Path file) throws IOException {
		try (final var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
			ByteBuffer buf = ByteBuffer.allocateDirect((int) channel.size()).order(ByteOrder.BIG_ENDIAN);
			channel.read(buf);
			return buf.flip();
		}
	}

	/**
	 * Read a number of bytes from a byte buffer returning a sliced sub buffer for those bytes and skipping over then in
	 * the byte buffer position stream.
	 *
	 * @param dataBuf
	 * 		The byte buffer to read from at current position
	 * @param length
	 * 		The number of bytes to read.
	 * @return new sliced byte buffer on the set of bytes between current position and  position + length
	 */
	public static ByteBuffer readByteBufferSlice(ByteBuffer dataBuf, int length) {
		final ByteBuffer sliceBuffer = dataBuf.slice(dataBuf.position(), length);
		dataBuf.position(dataBuf.position() + length); // skip
		return sliceBuffer;
	}

	public static Instant getInstantFromFileName(String fileName) {
		final String fileNameWithoutExt = fileName.substring(0, fileName.lastIndexOf('Z')+1);
		return Instant.from(formatter.parse(fileNameWithoutExt));
	}

	public static String getEpocNanosFromFileName(String fileName) {
		final Instant instant = getInstantFromFileName(fileName);
		return getEpocNanos(instant.getEpochSecond(), instant.getNano());
	}
	public static long getEpocNanosFromUrl(URL url) {
		return Long.parseLong(getEpocNanosFromFileName(Path.of(url.getFile()).getFileName().toString()));
	}

	public static String getEpocNanos2(long epocSeconds, long nanos) {
		final BigInteger bigIntegerNanosSinceEpoc = BigInteger
				.valueOf(epocSeconds)
				.multiply(SECONDS_TO_NANOSECONDS) // convert to nanoseconds
				.add(BigInteger.valueOf(nanos));
		return bigIntegerNanosSinceEpoc.toString();
	}

	public static Instant getInstantFromNanoEpicLong(long epicNanos) {
		long seconds = epicNanos / ((long)1e+9);
		long nanos = epicNanos - (seconds * ((long)1e+9));
		return Instant.ofEpochSecond(seconds, nanos);
	}
	public static long getEpocNanosAsLong(long epocSeconds, long nanos) {
		return (epocSeconds * (long)1e+9) + nanos;
	}

	// assume this is faster than big integer version, has to parse format every
	public static String getEpocNanos3(long epocSeconds, long nanos) {
		return String.format("%d%09d",epocSeconds,nanos);
	}

	// assume this is faster than string format version :-)
	public static String getEpocNanos(long epocSeconds, long nanos) {
		final String nanosString = Long.toString(nanos);
		return epocSeconds + "0".repeat(9-nanosString.length()) + nanosString;
	}

	public static byte[] hashShar384(ByteBuffer data) {
		try {
			final MessageDigest digest = MessageDigest.getInstance("SHA-384");
			if (data.hasArray()) {
				return digest.digest(data.array());
			} else {
				data.rewind();
				byte[] dataCopy = new byte[data.limit()];
				data.get(dataCopy);
				return digest.digest(dataCopy);
			}
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
