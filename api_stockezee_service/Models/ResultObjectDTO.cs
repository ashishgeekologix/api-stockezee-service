namespace api_stockezee_service.Models
{
    public class ResultObjectDTO<T>
    {
        public ResultType Result;

        public string ResultMessage;

        public T ResultData;

        public ResultObjectDTO()
        {
            Result = ResultType.Error;
            ResultMessage = string.Empty;
            ResultData = default(T);
        }
    }
    public enum ResultType { Error, Success }


}
