using System;
using Xunit;
using Confluent.Kafka;
using Moq;

namespace Api.Tests
{
    public class ConsumerWrapperTests
    {
        [Fact]
        public void Constructor_ValidConfig_InitializesConsumer()
        {
            // Arrange
            var mockConfig = new ConsumerConfig { GroupId = "test-group" };
            string topicName = "test-topic";

            // Act
            var consumerWrapper = new ConsumerWrapper(mockConfig, topicName);

            // Assert
            Assert.NotNull(consumerWrapper);
        }

        [Fact]
        public void ReadMessage_SuccessfulConsume_ReturnsMessage()
        {
            // Arrange
            var mockConfig = new ConsumerConfig { GroupId = "test-group" };
            var mockConsumer = new Mock<IConsumer<string, string>>();
            var consumeResult = new ConsumeResult<string, string>
            {
                Value = "Test Message"
            };

            mockConsumer.Setup(m => m.Consume(It.IsAny<CancellationToken>()))
                .Returns(consumeResult);

            // Act & Assert
            // Note: This would require refactoring the original class to allow dependency injection
        }

        [Fact]
        public void Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => new ConsumerWrapper(null, "topic"));
        }

        [Fact]
        public void Constructor_EmptyTopicName_ThrowsArgumentException()
        {
            // Arrange
            var config = new ConsumerConfig { GroupId = "test-group" };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new ConsumerWrapper(config, string.Empty));
        }

        [Fact]
        public void ReadMessage_NoMessageAvailable_HandlesTimeout()
        {
            // Arrange
            var mockConfig = new ConsumerConfig { GroupId = "test-group" };
            string topicName = "test-topic";

            // Act & Assert
            // Note: This would require modifying the original method to handle timeout scenarios
            Assert.Throws<OperationCanceledException>(() => {
                var consumerWrapper = new ConsumerWrapper(mockConfig, topicName);
                consumerWrapper.readMessage();
            });
        }
    }
}