using System.IO.Compression;
using System.Text;

namespace api_stockezee_service.Service
{
    public static class CompressionHelper
    {
        /// <summary>
        /// Compresses a UTF-8 string using GZip and encodes it as Base64.
        /// </summary>
        public static string CompressToBase64(string text)
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            using var output = new MemoryStream();
            using (var gzip = new GZipStream(output, CompressionLevel.Optimal))
            {
                gzip.Write(bytes, 0, bytes.Length);
            }
            return Convert.ToBase64String(output.ToArray());
        }

        /// <summary>
        /// Decompresses a Base64-encoded GZip string back into a UTF-8 string.
        /// </summary>
        public static string DecompressFromBase64(string base64Compressed)
        {
            try
            {
                var compressedBytes = Convert.FromBase64String(base64Compressed);
                using var input = new MemoryStream(compressedBytes);
                using var gzip = new GZipStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream();
                gzip.CopyTo(output);
                return Encoding.UTF8.GetString(output.ToArray());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return null;
        }

    }
}
